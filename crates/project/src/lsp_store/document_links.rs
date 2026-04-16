use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clock::Global;
use collections::HashMap;
use futures::FutureExt as _;
use futures::future::{Shared, join_all};
use gpui::{AppContext as _, Context, Entity, Task};
use language::Buffer;
use lsp::LanguageServerId;
use lsp::request::DocumentLinkResolve;
use settings::Settings as _;
use text::{Anchor, ToOffset as _, ToPointUtf16 as _};

use crate::lsp_command::{GetDocumentLinks, LspCommand as _};
use crate::lsp_store::LspStore;
use crate::project_settings::ProjectSettings;

#[derive(Clone, Debug)]
pub struct LspDocumentLink {
    pub range: Range<Anchor>,
    pub target: Option<String>,
    pub tooltip: Option<String>,
    pub data: Option<serde_json::Value>,
}

pub(super) type DocumentLinksTask =
    Shared<Task<std::result::Result<Vec<LspDocumentLink>, Arc<anyhow::Error>>>>;

#[derive(Debug, Default)]
pub(super) struct DocumentLinksData {
    pub(super) links: HashMap<LanguageServerId, Vec<LspDocumentLink>>,
    links_update: Option<(Global, DocumentLinksTask)>,
}

impl LspStore {
    pub fn document_links_for_buffer(
        &self,
        buffer_id: text::BufferId,
    ) -> Option<Vec<LspDocumentLink>> {
        let data = self.lsp_data.get(&buffer_id)?;
        let document_links = data.document_links.as_ref()?;
        Some(document_links.links.values().flatten().cloned().collect())
    }

    pub fn fetch_document_links(
        &mut self,
        buffer: &Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Vec<LspDocumentLink>> {
        let version_queried_for = buffer.read(cx).version();
        let buffer_id = buffer.read(cx).remote_id();

        let current_language_servers = self.as_local().map(|local| {
            local
                .buffers_opened_in_servers
                .get(&buffer_id)
                .cloned()
                .unwrap_or_default()
        });

        if let Some(lsp_data) = self.current_lsp_data(buffer_id) {
            if let Some(cached) = &lsp_data.document_links {
                if !version_queried_for.changed_since(&lsp_data.buffer_version) {
                    let has_different_servers =
                        current_language_servers.is_some_and(|current_language_servers| {
                            current_language_servers != cached.links.keys().copied().collect()
                        });
                    if !has_different_servers {
                        return Task::ready(cached.links.values().flatten().cloned().collect());
                    }
                }
            }
        }

        let links_lsp_data = self
            .latest_lsp_data(buffer, cx)
            .document_links
            .get_or_insert_default();
        if let Some((updating_for, running_update)) = &links_lsp_data.links_update {
            if !version_queried_for.changed_since(updating_for) {
                let running = running_update.clone();
                return cx.background_spawn(async move { running.await.unwrap_or_default() });
            }
        }

        let buffer = buffer.clone();
        let query_version = version_queried_for.clone();
        let new_task = cx
            .spawn(async move |lsp_store, cx| {
                cx.background_executor()
                    .timer(Duration::from_millis(30))
                    .await;

                let fetched = lsp_store
                    .update(cx, |lsp_store, cx| {
                        lsp_store.fetch_document_links_for_buffer(&buffer, cx)
                    })
                    .map_err(Arc::new)?
                    .await
                    .context("fetching document links")
                    .map_err(Arc::new);

                let fetched = match fetched {
                    Ok(fetched) => fetched,
                    Err(e) => {
                        lsp_store
                            .update(cx, |lsp_store, _| {
                                if let Some(lsp_data) = lsp_store.lsp_data.get_mut(&buffer_id) {
                                    if let Some(document_links) = &mut lsp_data.document_links {
                                        document_links.links_update = None;
                                    }
                                }
                            })
                            .ok();
                        return Err(e);
                    }
                };

                lsp_store
                    .update(cx, |lsp_store, cx| {
                        let lsp_data = lsp_store.latest_lsp_data(&buffer, cx);
                        let links_data = lsp_data.document_links.get_or_insert_default();

                        if let Some(fetched_links) = fetched {
                            if lsp_data.buffer_version == query_version {
                                links_data.links.extend(fetched_links);
                            } else if !lsp_data.buffer_version.changed_since(&query_version) {
                                lsp_data.buffer_version = query_version;
                                links_data.links = fetched_links;
                            }
                        }
                        links_data.links_update = None;
                        links_data.links.values().flatten().cloned().collect()
                    })
                    .map_err(Arc::new)
            })
            .shared();

        links_lsp_data.links_update = Some((version_queried_for, new_task.clone()));

        cx.background_spawn(async move { new_task.await.unwrap_or_default() })
    }

    fn fetch_document_links_for_buffer(
        &mut self,
        buffer: &Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<Option<HashMap<LanguageServerId, Vec<LspDocumentLink>>>>> {
        if let Some((client, project_id)) = self.upstream_client() {
            let request = GetDocumentLinks;
            if !self.is_capable_for_proto_request(buffer, &request, cx) {
                return Task::ready(Ok(None));
            }

            let request_timeout = ProjectSettings::get_global(cx)
                .global_lsp_settings
                .get_request_timeout();
            let request_task = client.request_lsp(
                project_id,
                None,
                request_timeout,
                cx.background_executor().clone(),
                request.to_proto(project_id, buffer.read(cx)),
            );
            let buffer = buffer.clone();
            cx.spawn(async move |weak_lsp_store, cx| {
                let Some(lsp_store) = weak_lsp_store.upgrade() else {
                    return Ok(None);
                };
                let Some(responses) = request_task.await? else {
                    return Ok(None);
                };

                let document_links = join_all(responses.payload.into_iter().map(|response| {
                    let lsp_store = lsp_store.clone();
                    let buffer = buffer.clone();
                    let cx = cx.clone();
                    async move {
                        (
                            LanguageServerId::from_proto(response.server_id),
                            GetDocumentLinks
                                .response_from_proto(response.response, lsp_store, buffer, cx)
                                .await,
                        )
                    }
                }))
                .await;

                let mut has_errors = false;
                let result = document_links
                    .into_iter()
                    .filter_map(|(server_id, links)| match links {
                        Ok(links) => Some((server_id, links)),
                        Err(e) => {
                            has_errors = true;
                            log::error!("Failed to fetch document links: {e:#}");
                            None
                        }
                    })
                    .collect::<HashMap<_, _>>();
                anyhow::ensure!(
                    !has_errors || !result.is_empty(),
                    "Failed to fetch document links"
                );
                Ok(Some(result))
            })
        } else {
            let links_task =
                self.request_multiple_lsp_locally(buffer, None::<usize>, GetDocumentLinks, cx);
            cx.background_spawn(async move { Ok(Some(links_task.await.into_iter().collect())) })
        }
    }

    pub fn resolve_visible_document_links(
        &mut self,
        buffer: &Entity<Buffer>,
        visible_range: Range<Anchor>,
        cx: &mut Context<Self>,
    ) -> Task<Vec<LspDocumentLink>> {
        let buffer_id = buffer.read(cx).remote_id();
        let snapshot = buffer.read(cx).snapshot();
        let visible_start = visible_range.start.to_offset(&snapshot);
        let visible_end = visible_range.end.to_offset(&snapshot);

        let Some(document_links) = self
            .lsp_data
            .get(&buffer_id)
            .and_then(|data| data.document_links.as_ref())
        else {
            return Task::ready(Vec::new());
        };

        let capable_servers = document_links
            .links
            .keys()
            .filter_map(|server_id| {
                let server = self.language_server_for_id(*server_id)?;
                can_resolve_link(&server.capabilities()).then_some((*server_id, server))
            })
            .collect::<HashMap<_, _>>();
        if capable_servers.is_empty() {
            return Task::ready(Vec::new());
        }

        let to_resolve = document_links
            .links
            .iter()
            .flat_map(|(server_id, links)| {
                let snapshot = &snapshot;
                links.iter().enumerate().filter_map(move |(idx, link)| {
                    if link.target.is_some() {
                        return None;
                    }
                    let offset = link.range.start.to_offset(&snapshot);
                    if offset < visible_start || offset > visible_end {
                        return None;
                    }
                    let lsp_link = lsp::DocumentLink {
                        range: lsp::Range {
                            start: point_to_lsp(link.range.start.to_point_utf16(&snapshot)),
                            end: point_to_lsp(link.range.end.to_point_utf16(&snapshot)),
                        },
                        target: None,
                        tooltip: link.tooltip.clone(),
                        data: link.data.clone(),
                    };
                    Some((*server_id, idx, lsp_link))
                })
            })
            .collect::<Vec<_>>();
        if to_resolve.is_empty() {
            return Task::ready(Vec::new());
        }

        let request_timeout = ProjectSettings::get_global(cx)
            .global_lsp_settings
            .get_request_timeout();

        cx.spawn(async move |lsp_store, cx| {
            let mut resolved = Vec::new();
            for (server_id, index, lsp_link) in to_resolve {
                let Some(server) = capable_servers.get(&server_id) else {
                    continue;
                };
                match server
                    .request::<DocumentLinkResolve>(lsp_link, request_timeout)
                    .await
                    .into_response()
                {
                    Ok(resolved_link) => resolved.push((server_id, index, resolved_link)),
                    Err(e) => log::warn!("Failed to resolve document link: {e:#}"),
                }
            }
            if resolved.is_empty() {
                return Vec::new();
            }

            lsp_store
                .update(cx, |lsp_store, _| {
                    let Some(document_links) = lsp_store
                        .lsp_data
                        .get_mut(&buffer_id)
                        .and_then(|data| data.document_links.as_mut())
                    else {
                        return Vec::new();
                    };
                    let mut newly_resolved = Vec::new();
                    for (server_id, index, resolved_link) in resolved {
                        if let Some(links) = document_links.links.get_mut(&server_id) {
                            if let Some(link) = links.get_mut(index) {
                                link.target = resolved_link.target.map(|u| u.to_string());
                                if let Some(tooltip) = resolved_link.tooltip {
                                    link.tooltip = Some(tooltip);
                                }
                                link.data = resolved_link.data;
                                newly_resolved.push(link.clone());
                            }
                        }
                    }
                    newly_resolved
                })
                .unwrap_or_default()
        })
    }
}

fn can_resolve_link(capabilities: &lsp::ServerCapabilities) -> bool {
    capabilities
        .document_link_provider
        .as_ref()
        .and_then(|opts| opts.resolve_provider)
        .unwrap_or(false)
}

fn point_to_lsp(point: text::PointUtf16) -> lsp::Position {
    lsp::Position::new(point.row, point.column)
}
