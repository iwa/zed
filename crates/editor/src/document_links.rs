use futures::future::join_all;
use itertools::Itertools;
use project::lsp_store::LspDocumentLink;
use text::BufferId;
use ui::Context;

use settings::Settings;

use crate::{Editor, LSP_REQUEST_DEBOUNCE_TIMEOUT, editor_settings::EditorSettings};

impl Editor {
    pub(super) fn refresh_document_links(
        &mut self,
        for_buffer: Option<BufferId>,
        cx: &mut Context<Self>,
    ) {
        if !self.lsp_data_enabled() || !EditorSettings::get_global(cx).lsp_document_links {
            return;
        }
        let Some(project) = self.project.clone() else {
            return;
        };

        let buffers_to_query = self
            .visible_buffers(cx)
            .into_iter()
            .filter(|buffer| self.is_lsp_relevant(buffer.read(cx).file(), cx))
            .chain(for_buffer.and_then(|id| self.buffer.read(cx).buffer(id)))
            .filter(|buffer| {
                let id = buffer.read(cx).remote_id();
                (for_buffer.is_none_or(|target| target == id))
                    && self.registered_buffers.contains_key(&id)
            })
            .unique_by(|buffer| buffer.read(cx).remote_id())
            .collect::<Vec<_>>();

        self.refresh_document_links_task = cx.spawn(async move |editor, cx| {
            cx.background_executor()
                .timer(LSP_REQUEST_DEBOUNCE_TIMEOUT)
                .await;

            let Some(tasks) = editor
                .update(cx, |_, cx| {
                    project.read(cx).lsp_store().update(cx, |lsp_store, cx| {
                        buffers_to_query
                            .into_iter()
                            .map(|buffer| {
                                let buffer_id = buffer.read(cx).remote_id();
                                let task = lsp_store.fetch_document_links(&buffer, cx);
                                async move { (buffer_id, task.await) }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .ok()
            else {
                return;
            };

            let results = join_all(tasks).await;
            if results.is_empty() {
                return;
            }

            editor
                .update(cx, |editor, cx| {
                    for (buffer_id, links) in results {
                        if links.is_empty() {
                            editor.lsp_document_links.remove(&buffer_id);
                        } else {
                            editor.lsp_document_links.insert(buffer_id, links);
                        }
                    }
                    editor.resolve_visible_document_links(cx);
                    cx.notify();
                })
                .ok();
        });
    }

    pub(super) fn resolve_visible_document_links(&mut self, cx: &mut Context<Self>) {
        if !self.lsp_data_enabled() || !EditorSettings::get_global(cx).lsp_document_links {
            return;
        }
        let Some(project) = self.project.clone() else {
            return;
        };

        let resolve_tasks = self
            .visible_buffer_ranges(cx)
            .into_iter()
            .filter_map(|(snapshot, visible_range, _)| {
                let buffer_id = snapshot.remote_id();
                let buffer = self.buffer.read(cx).buffer(buffer_id)?;
                let visible_anchor_range = snapshot.anchor_before(visible_range.start)
                    ..snapshot.anchor_after(visible_range.end);
                let task = project.update(cx, |project, cx| {
                    project.lsp_store().update(cx, |lsp_store, cx| {
                        lsp_store.resolve_visible_document_links(&buffer, visible_anchor_range, cx)
                    })
                });
                Some((buffer_id, task))
            })
            .collect::<Vec<_>>();
        if resolve_tasks.is_empty() {
            return;
        }

        let buffer_ids: Vec<_> = resolve_tasks.iter().map(|(id, _)| *id).collect();
        self.resolve_document_links_task = cx.spawn(async move |editor, cx| {
            let resolved = join_all(
                resolve_tasks
                    .into_iter()
                    .map(|(buffer_id, task)| async move { (buffer_id, task.await) }),
            )
            .await;

            let has_newly_resolved = resolved.iter().any(|(_, links)| !links.is_empty());
            if !has_newly_resolved {
                return;
            }

            editor
                .update(cx, |editor, cx| {
                    let lsp_store = project.read(cx).lsp_store().read(cx);
                    for buffer_id in &buffer_ids {
                        if let Some(all_links) = lsp_store.document_links_for_buffer(*buffer_id) {
                            editor.lsp_document_links.insert(*buffer_id, all_links);
                        }
                    }
                    cx.notify();
                })
                .ok();
        });
    }

    pub(crate) fn document_link_at(
        &self,
        buffer_id: BufferId,
        position: &text::Anchor,
        snapshot: &language::BufferSnapshot,
    ) -> Option<&LspDocumentLink> {
        self.lsp_document_links
            .get(&buffer_id)?
            .iter()
            .find(|link| {
                link.range.start.cmp(position, snapshot).is_le()
                    && link.range.end.cmp(position, snapshot).is_ge()
            })
    }
}
