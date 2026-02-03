document.addEventListener('DOMContentLoaded', function () {
    const loadingEl = document.getElementById('metadata-loading');
    const contentEl = document.getElementById('metadata-content');
    const errorEl = document.getElementById('metadata-error');
    const errorMsgEl = document.getElementById('metadata-error-message');
    const retryBtn = document.getElementById('metadata-retry');
    const refreshBtn = document.getElementById('metadata-refresh');
    const searchInput = document.getElementById('metadata-search');
    const tableBody = document.getElementById('metadata-table-body');
    const totalEl = document.getElementById('metadata-total');
    const arrCountEl = document.getElementById('metadata-arr-count');
    const arrListEl = document.getElementById('metadata-arr-list');

    const editModal = document.getElementById('metadata-edit-modal');
    const editTitle = document.getElementById('metadata-edit-title');
    const editArrInput = document.getElementById('metadata-edit-arr');
    const editSaveBtn = document.getElementById('metadata-edit-save');
    const editCancelBtn = document.getElementById('metadata-edit-cancel');

    let mappings = [];
    let activeMapping = null;

    function showError(message) {
        loadingEl.style.display = 'none';
        contentEl.style.display = 'none';
        errorEl.style.display = 'flex';
        errorMsgEl.textContent = message;
    }

    function showContent() {
        loadingEl.style.display = 'none';
        errorEl.style.display = 'none';
        contentEl.style.display = 'block';
    }

    function showLoading() {
        contentEl.style.display = 'none';
        errorEl.style.display = 'none';
        loadingEl.style.display = 'block';
    }

    function formatDate(value) {
        if (!value) return '-';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return date.toLocaleString();
    }

    function setStats(stats) {
        totalEl.textContent = stats.total ?? '-';
        const byArr = stats.by_arr || {};
        const names = Object.keys(byArr).sort();
        arrCountEl.textContent = names.length.toString();
        arrListEl.textContent = names.length ? names.join(', ') : '-';
    }

    function renderTable() {
        const query = searchInput.value.trim().toLowerCase();
        const filtered = query
            ? mappings.filter((m) => {
                return (m.infohash || '').toLowerCase().includes(query)
                    || (m.torrent_name || '').toLowerCase().includes(query)
                    || (m.arr_name || '').toLowerCase().includes(query);
            })
            : mappings;

        if (!filtered.length) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="5" class="text-center text-base-content/70 py-8">No mappings found.</td>
                </tr>
            `;
            return;
        }

        tableBody.innerHTML = filtered.map((m) => `
            <tr>
                <td class="font-mono text-xs break-all">${m.infohash || '-'}</td>
                <td>${m.torrent_name || '-'}</td>
                <td><span class="badge badge-outline">${m.arr_name || '-'}</span></td>
                <td>${formatDate(m.updated_at)}</td>
                <td class="text-right">
                    <div class="flex justify-end gap-2">
                        <button class="btn btn-xs btn-outline" data-action="edit" data-infohash="${m.infohash}">
                            <i class="bi bi-pencil"></i>
                        </button>
                        <button class="btn btn-xs btn-outline btn-error" data-action="delete" data-infohash="${m.infohash}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `).join('');
    }

    async function loadStats() {
        const response = await window.decypharrUtils.fetcher('api/metadata/stats');
        if (!response.ok) {
            throw new Error('Failed to load stats');
        }
        const data = await response.json();
        setStats(data);
    }

    async function loadMappings() {
        const response = await window.decypharrUtils.fetcher('api/metadata/list');
        if (!response.ok) {
            throw new Error('Failed to load mappings');
        }
        mappings = await response.json();
        renderTable();
    }

    async function refreshAll() {
        showLoading();
        try {
            await Promise.all([loadStats(), loadMappings()]);
            showContent();
        } catch (err) {
            console.error(err);
            showError(err.message || 'Failed to load metadata');
        }
    }

    function openEditModal(mapping) {
        activeMapping = mapping;
        editTitle.textContent = mapping.torrent_name || mapping.infohash;
        editArrInput.value = mapping.arr_name || '';
        editModal.showModal();
    }

    async function saveEdit() {
        if (!activeMapping) return;
        const arrName = editArrInput.value.trim();
        if (!arrName) {
            window.decypharrUtils.createToast('Arr name is required.', 'warning');
            return;
        }
        const payload = {
            infohash: activeMapping.infohash,
            torrent_id: activeMapping.torrent_id,
            torrent_name: activeMapping.torrent_name,
            arr_name: arrName
        };
        const response = await window.decypharrUtils.fetcher('api/metadata/set', {
            method: 'POST',
            body: JSON.stringify(payload)
        });
        if (!response.ok) {
            const text = await response.text();
            throw new Error(text || 'Failed to update mapping');
        }
        window.decypharrUtils.createToast('Mapping updated.', 'success');
        editModal.close();
        await refreshAll();
    }

    async function deleteMapping(infohash) {
        if (!infohash) return;
        if (!confirm('Delete this mapping?')) return;
        const response = await window.decypharrUtils.fetcher(`api/metadata/${infohash}`, {
            method: 'DELETE'
        });
        if (!response.ok) {
            const text = await response.text();
            throw new Error(text || 'Failed to delete mapping');
        }
        window.decypharrUtils.createToast('Mapping deleted.', 'success');
        await refreshAll();
    }

    tableBody.addEventListener('click', async (event) => {
        const button = event.target.closest('button');
        if (!button) return;
        const action = button.dataset.action;
        const infohash = button.dataset.infohash;
        const mapping = mappings.find((m) => m.infohash === infohash);
        try {
            if (action === 'edit' && mapping) {
                openEditModal(mapping);
            }
            if (action === 'delete') {
                await deleteMapping(infohash);
            }
        } catch (err) {
            console.error(err);
            window.decypharrUtils.createToast(err.message || 'Action failed', 'error');
        }
    });

    editSaveBtn.addEventListener('click', async () => {
        try {
            await saveEdit();
        } catch (err) {
            console.error(err);
            window.decypharrUtils.createToast(err.message || 'Failed to save', 'error');
        }
    });

    editCancelBtn.addEventListener('click', () => {
        editModal.close();
    });

    refreshBtn.addEventListener('click', refreshAll);
    retryBtn.addEventListener('click', refreshAll);
    searchInput.addEventListener('input', renderTable);

    refreshAll();
});
