// WebDAV-style File Browser with URL-based navigation
class FileBrowser {
    constructor() {
        this.state = {
            currentPath: '/',
            currentPage: 1,
            itemsPerPage: 20,
            searchQuery: '',
            entries: [],
            total: 0,
            totalPages: 0,
            parentDir: null,
            selectedEntry: null,
            selectedEntries: new Set()
        };

        this.refs = {
            breadcrumbNav: document.getElementById('breadcrumbNav'),
            refreshBtn: document.getElementById('refreshBtn'),
            searchInput: document.getElementById('searchInput'),
            pageSizeSelect: document.getElementById('pageSizeSelect'),
            fileBrowserList: document.getElementById('fileBrowserList'),
            paginationInfo: document.getElementById('paginationInfo'),
            paginationControls: document.getElementById('paginationControls'),
            emptyState: document.getElementById('emptyState'),

            // Bulk actions
            selectAllCheckbox: document.getElementById('selectAllCheckbox'),
            bulkActionsBar: document.getElementById('bulkActionsBar'),
            selectedCount: document.getElementById('selectedCount'),
            bulkDownloadBtn: document.getElementById('bulkDownloadBtn'),
            bulkMoveBtn: document.getElementById('bulkMoveBtn'),
            bulkDeleteBtn: document.getElementById('bulkDeleteBtn'),
            clearSelectionBtn: document.getElementById('clearSelectionBtn'),

            // Modals
            moveTorrentModal: document.getElementById('moveTorrentModal'),
            moveTorrentName: document.getElementById('moveTorrentName'),
            moveCurrentDebrid: document.getElementById('moveCurrentDebrid'),
            moveTargetDebrid: document.getElementById('moveTargetDebrid'),
            moveKeepSource: document.getElementById('moveKeepSource'),
            moveWaitComplete: document.getElementById('moveWaitComplete'),
            confirmMoveBtn: document.getElementById('confirmMoveBtn'),

            // Context menu
            contextMenu: document.getElementById('contextMenu'),
            contextDownload: document.getElementById('contextDownload'),
            contextMove: document.getElementById('contextMove'),
            contextDelete: document.getElementById('contextDelete')
        };

        this.currentMoveTarget = null;
        this.searchTimeout = null;

        this.init();
    }

    init() {
        this.bindEvents();
        this.loadStateFromURL();
        this.loadEntries();
    }

    bindEvents() {
        // Refresh
        this.refs.refreshBtn.addEventListener('click', () => this.refresh());

        // Search with debounce
        this.refs.searchInput.addEventListener('input', (e) => {
            clearTimeout(this.searchTimeout);
            this.searchTimeout = setTimeout(() => {
                this.state.searchQuery = e.target.value;
                this.state.currentPage = 1;
                this.updateURL();
                this.refresh();
            }, 300);
        });

        // Page size
        this.refs.pageSizeSelect.addEventListener('change', (e) => {
            this.state.itemsPerPage = parseInt(e.target.value);
            this.state.currentPage = 1;
            this.updateURL();
            this.refresh();
        });

        // Select all checkbox
        if (this.refs.selectAllCheckbox) {
            this.refs.selectAllCheckbox.addEventListener('change', (e) => {
                this.handleSelectAll(e.target.checked);
            });
        }

        // Bulk action buttons
        if (this.refs.bulkDownloadBtn) {
            this.refs.bulkDownloadBtn.addEventListener('click', () => this.bulkDownload());
        }
        if (this.refs.bulkMoveBtn) {
            this.refs.bulkMoveBtn.addEventListener('click', () => this.bulkMove());
        }
        if (this.refs.bulkDeleteBtn) {
            this.refs.bulkDeleteBtn.addEventListener('click', () => this.bulkDelete());
        }
        if (this.refs.clearSelectionBtn) {
            this.refs.clearSelectionBtn.addEventListener('click', () => this.clearSelection());
        }

        // Move torrent
        if (this.refs.confirmMoveBtn) {
            this.refs.confirmMoveBtn.addEventListener('click', () => this.executeTorrentMove());
        }

        // Hide context menu on click outside
        document.addEventListener('click', (e) => {
            if (!this.refs.contextMenu.contains(e.target)) {
                this.hideContextMenu();
            }
        });

        // Prevent default context menu
        document.addEventListener('contextmenu', (e) => {
            const row = e.target.closest('tr[data-entry]');
            if (row) {
                e.preventDefault();
            }
        });

        // Handle browser back/forward
        window.addEventListener('popstate', () => {
            this.loadStateFromURL();
            this.refresh();
        });
    }

    loadStateFromURL() {
        const params = new URLSearchParams(window.location.search);

        // Load path from URL
        this.state.currentPath = params.get('path') || '/';

        // Load page from URL
        const page = parseInt(params.get('page'));
        this.state.currentPage = page > 0 ? page : 1;

        // Load search from URL
        this.state.searchQuery = params.get('search') || '';
        if (this.refs.searchInput) {
            this.refs.searchInput.value = this.state.searchQuery;
        }

        // Load page size from URL
        const limit = parseInt(params.get('limit'));
        this.state.itemsPerPage = limit > 0 ? limit : 20;
        if (this.refs.pageSizeSelect) {
            this.refs.pageSizeSelect.value = this.state.itemsPerPage.toString();
        }
    }

    updateURL() {
        const params = new URLSearchParams();

        // Always include path
        if (this.state.currentPath !== '/') {
            params.set('path', this.state.currentPath);
        }

        // Include page if not 1
        if (this.state.currentPage > 1) {
            params.set('page', this.state.currentPage);
        }

        // Include search if present
        if (this.state.searchQuery) {
            params.set('search', this.state.searchQuery);
        }

        // Include limit if not default
        if (this.state.itemsPerPage !== 20) {
            params.set('limit', this.state.itemsPerPage);
        }

        const newURL = `${window.location.pathname}${params.toString() ? '?' + params.toString() : ''}`;
        window.history.pushState({}, '', newURL);
    }

    navigate(path) {
        this.state.currentPath = path;
        this.state.currentPage = 1;
        this.updateURL();
        this.loadEntries();
    }

    refresh() {
        this.loadEntries();
    }

    async loadEntries() {
        try {
            // Build API URL based on path depth
            const pathParts = this.state.currentPath.split('/').filter(p => p);
            let apiUrl = `${window.urlBase}api/browse`;

            if (pathParts.length === 0) {
                apiUrl += '/';
            } else if (pathParts.length === 1) {
                apiUrl += `/${encodeURIComponent(pathParts[0])}`;
            } else if (pathParts.length === 2) {
                apiUrl += `/${encodeURIComponent(pathParts[0])}/${encodeURIComponent(pathParts[1])}`;
            } else if (pathParts.length === 3) {
                apiUrl += `/${encodeURIComponent(pathParts[0])}/${encodeURIComponent(pathParts[1])}/${encodeURIComponent(pathParts[2])}`;
            }

            // Add query params
            const params = new URLSearchParams({
                page: this.state.currentPage,
                limit: this.state.itemsPerPage
            });

            if (this.state.searchQuery) {
                params.set('search', this.state.searchQuery);
            }

            const response = await fetch(`${apiUrl}?${params}`);
            if (!response.ok) throw new Error('Failed to load directory');

            const data = await response.json();
            this.state.entries = data.entries || [];
            this.state.total = data.total || 0;
            this.state.totalPages = data.total_pages || 0;
            this.state.parentDir = data.parent_dir;

            this.updateBreadcrumbs();
            this.renderEntries();
            this.renderPagination();
        } catch (error) {
            console.error('Error loading entries:', error);
            window.createToast('Failed to load directory', 'error');
        }
    }

    updateBreadcrumbs() {
        const parts = this.state.currentPath.split('/').filter(p => p);

        let html = `<li><a href="${window.urlBase}browse" data-path="/">
            <i class="bi bi-house-door"></i> Home
        </a></li>`;

        let currentPath = '';
        parts.forEach(part => {
            currentPath += '/' + part;
            const displayName = decodeURIComponent(part);
            html += `<li><a href="${window.urlBase}browse?path=${encodeURIComponent(currentPath)}" data-path="${currentPath}">${this.escapeHtml(displayName)}</a></li>`;
        });

        this.refs.breadcrumbNav.innerHTML = html;

        // Add click handlers to override default link behavior
        this.refs.breadcrumbNav.querySelectorAll('a').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const path = e.currentTarget.dataset.path;
                this.navigate(path);
            });
        });
    }

    renderEntries() {
        if (this.state.entries.length === 0) {
            this.refs.fileBrowserList.innerHTML = '';
            this.refs.emptyState.classList.remove('hidden');
            this.refs.paginationInfo.textContent = 'No items found';
            return;
        }

        this.refs.emptyState.classList.add('hidden');

        this.refs.fileBrowserList.innerHTML = this.state.entries.map(entry => {
            const icon = entry.is_dir ?
                '<i class="bi bi-folder-fill text-warning text-lg transition-colors group-hover:text-warning-content"></i>' :
                '<i class="bi bi-file-earmark text-info transition-colors group-hover:text-info-content"></i>';

            const entryId = entry.info_hash || entry.path;
            const isChecked = this.state.selectedEntries.has(entryId);

            return `
                <tr class="group hover:bg-base-200 transition-colors"
                    data-entry='${JSON.stringify(entry)}'
                    data-entry-id="${this.escapeAttr(entryId)}"
                    oncontextmenu="window.fileBrowser.showContextMenu(event, ${this.escapeAttr(JSON.stringify(entry))});">
                    <td onclick="event.stopPropagation();">
                        <label class="cursor-pointer">
                            <input type="checkbox"
                                   class="checkbox checkbox-sm checkbox-primary entry-checkbox"
                                   data-entry-id="${this.escapeAttr(entryId)}"
                                   ${isChecked ? 'checked' : ''}
                                   onchange="window.fileBrowser.handleEntrySelect('${this.escapeAttr(entryId)}', this.checked, ${this.escapeAttr(JSON.stringify(entry))})">
                        </label>
                    </td>
                    <td>${icon}</td>
                    <td onclick="window.fileBrowser.handleEntryClick('${this.escapeJs(entry.path)}', ${entry.is_dir}, '${this.escapeJs(entry.name)}');" class="cursor-pointer hover:text-primary transition-colors">
                        <span class="font-medium">${this.escapeHtml(entry.name)}</span>
                    </td>
                    <td>
                        ${entry.size <= 0 ? '-' : this.formatSize(entry.size)}
                    </td>
                    <td class="text-xs text-base-content/70">
                        ${entry.mod_time || '-'}
                    </td>
                    <td>
                        ${entry.active_debrid ? `<span>${this.escapeHtml(entry.active_debrid)}</span>` : '-'}
                    </td>
                    <td onclick="event.stopPropagation();">
                        <div class="dropdown dropdown-end">
                            <label tabindex="0" class="btn btn-ghost btn-xs">
                                <i class="bi bi-three-dots-vertical"></i>
                            </label>
                            <ul tabindex="0" class="dropdown-content menu p-2 shadow bg-base-200 rounded-box w-52 z-50">
                                ${!entry.is_dir ? `
                                    <li><a onclick="window.fileBrowser.downloadFile('${this.escapeJs(entry.path)}', '${this.escapeJs(entry.name)}')">
                                        <i class="bi bi-download"></i> Download
                                    </a></li>
                                ` : ''}
                                ${entry.can_delete ? `
                                    <li><a onclick="window.fileBrowser.showMoveModal('${this.escapeJs(entry.info_hash)}')">
                                        <i class="bi bi-arrow-left-right"></i> Switch Provider
                                    </a></li>
                                    <li><a onclick="window.fileBrowser.deleteTorrent('${this.escapeJs(entry.info_hash)}', '${this.escapeJs(entry.name)}')" class="text-error">
                                        <i class="bi bi-trash"></i> Delete
                                    </a></li>
                                ` : ''}
                            </ul>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');

        this.updateSelectionUI();
    }

    renderPagination() {
        const start = (this.state.currentPage - 1) * this.state.itemsPerPage + 1;
        const end = Math.min(start + this.state.itemsPerPage - 1, this.state.total);

        this.refs.paginationInfo.textContent = this.state.total > 0
            ? `Showing ${start}-${end} of ${this.state.total} items`
            : 'No items';

        if (this.state.totalPages <= 1) {
            this.refs.paginationControls.innerHTML = '';
            return;
        }

        let html = `
            <button class="join-item btn btn-sm ${this.state.currentPage === 1 ? 'btn-disabled' : ''}"
                    onclick="window.fileBrowser.goToPage(${this.state.currentPage - 1})"
                    ${this.state.currentPage === 1 ? 'disabled' : ''}>
                <i class="bi bi-chevron-left"></i>
            </button>
        `;

        // Smart pagination: show first, last, current, and nearby pages
        for (let i = 1; i <= this.state.totalPages; i++) {
            if (i === 1 || i === this.state.totalPages ||
                (i >= this.state.currentPage - 2 && i <= this.state.currentPage + 2)) {
                html += `
                    <button class="join-item btn btn-sm ${i === this.state.currentPage ? 'btn-active' : ''}"
                            onclick="window.fileBrowser.goToPage(${i})">${i}</button>
                `;
            } else if (i === this.state.currentPage - 3 || i === this.state.currentPage + 3) {
                html += `<button class="join-item btn btn-sm btn-disabled" disabled>...</button>`;
            }
        }

        html += `
            <button class="join-item btn btn-sm ${this.state.currentPage === this.state.totalPages ? 'btn-disabled' : ''}"
                    onclick="window.fileBrowser.goToPage(${this.state.currentPage + 1})"
                    ${this.state.currentPage === this.state.totalPages ? 'disabled' : ''}>
                <i class="bi bi-chevron-right"></i>
            </button>
        `;

        this.refs.paginationControls.innerHTML = html;
    }

    goToPage(page) {
        if (page < 1 || page > this.state.totalPages) return;
        this.state.currentPage = page;
        this.updateURL();
        this.refresh();
    }

    handleEntryClick(path, isDir, name) {
        if (isDir) {
            this.navigate(path);
        } else {
            this.downloadFile(path, name);
        }
    }

    showContextMenu(event, entry) {
        event.preventDefault();
        event.stopPropagation();

        // Position context menu
        this.refs.contextMenu.style.left = event.pageX + 'px';
        this.refs.contextMenu.style.top = event.pageY + 'px';
        this.refs.contextMenu.classList.remove('hidden');

        // Show/hide appropriate menu items
        if (!entry.is_dir) {
            this.refs.contextDownload.classList.remove('hidden');
            this.refs.contextDownload.onclick = () => this.downloadFile(entry.path, entry.name);
        } else {
            this.refs.contextDownload.classList.add('hidden');
        }

        if (entry.can_delete) {
            this.refs.contextMove.classList.remove('hidden');
            this.refs.contextDelete.classList.remove('hidden');
            this.refs.contextMove.onclick = () => this.showMoveModal(entry.info_hash);
            this.refs.contextDelete.onclick = () => this.deleteTorrent(entry.info_hash, entry.name);
        } else {
            this.refs.contextMove.classList.add('hidden');
            this.refs.contextDelete.classList.add('hidden');
        }

        this.state.selectedEntry = entry;
    }

    hideContextMenu() {
        this.refs.contextMenu.classList.add('hidden');
    }

    downloadFile(path, fileName) {
        this.hideContextMenu();

        // Extract torrent and file names from path
        const pathParts = path.split('/').filter(p => p);
        if (pathParts.length < 3) return;

        const torrentName = pathParts[pathParts.length - 2];
        const file = pathParts[pathParts.length - 1];

        const downloadUrl = `${window.urlBase}api/browse/download/${encodeURIComponent(torrentName)}/${encodeURIComponent(file)}`;
        window.open(downloadUrl, '_blank');
    }

    async showMoveModal(infoHash) {
        this.hideContextMenu();

        try {
            const response = await fetch(`${window.urlBase}api/browse/torrents/${infoHash}/info`);
            if (!response.ok) throw new Error('Failed to load item info');

            const torrent = await response.json();
            this.currentMoveTarget = torrent;

            this.refs.moveTorrentName.textContent = torrent.name;
            this.refs.moveCurrentDebrid.textContent = torrent.active_debrid || 'None';

            // Populate target debrid options
            this.refs.moveTargetDebrid.innerHTML = '<option disabled selected>Select debrid provider</option>';

            const configResponse = await fetch(`${window.urlBase}api/config`);
            if (configResponse.ok) {
                const config = await configResponse.json();
                config.debrids.forEach(debrid => {
                    if (debrid.name !== torrent.active_debrid) {
                        const option = document.createElement('option');
                        option.value = debrid.name;
                        option.textContent = debrid.name.charAt(0).toUpperCase() + debrid.name.slice(1);
                        this.refs.moveTargetDebrid.appendChild(option);
                    }
                });
            }

            this.refs.moveTorrentModal.showModal();
        } catch (error) {
            console.error('Error loading item info:', error);
            window.createToast('Failed to load item info', 'error');
        }
    }

    async executeTorrentMove() {
        if (!this.currentMoveTarget) return;

        const targetDebrid = this.refs.moveTargetDebrid.value;
        if (!targetDebrid) {
            window.createToast('Please select a target debrid', 'warning');
            return;
        }

        try {
            const response = await fetch(`${window.urlBase}api/browse/torrents/${this.currentMoveTarget.info_hash}/move`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    target_debrid: targetDebrid,
                    keep_source: this.refs.moveKeepSource.checked,
                    wait_complete: this.refs.moveWaitComplete.checked
                })
            });

            if (!response.ok) throw new Error('Failed to move torrent');

            this.refs.moveTorrentModal.close();
            window.createToast(`Migration started for ${this.currentMoveTarget.name}`, 'success');

            setTimeout(() => this.refresh(), 2000);
        } catch (error) {
            console.error('Error moving torrent:', error);
            window.createToast('Failed to move torrent', 'error');
        }
    }

    async deleteTorrent(infoHash, name) {
        this.hideContextMenu();

        if (!confirm(`Delete "${name}"?\n\nThis will remove the item from the system.`)) {
            return;
        }

        try {
            const response = await fetch(`${window.urlBase}api/browse/torrents/${infoHash}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Failed to delete entry');

            window.createToast('Item deleted successfully', 'success');
            this.refresh();
        } catch (error) {
            console.error('Error deleting item:', error);
            window.createToast('Failed to delete item', 'error');
        }
    }

    // Utility methods
    formatSize(bytes) {
        if (!bytes || bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    escapeAttr(text) {
        if (typeof text !== 'string') {
            text = JSON.stringify(text);
        }
        return text.replace(/'/g, '&#39;').replace(/"/g, '&quot;');
    }

    escapeJs(text) {
        if (typeof text !== 'string') {
            text = String(text);
        }
        return text.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
    }

    // Multi-select methods
    handleEntrySelect(entryId, checked, entry) {
        if (checked) {
            this.state.selectedEntries.add(entryId);
        } else {
            this.state.selectedEntries.delete(entryId);
        }
        this.updateSelectionUI();
    }

    handleSelectAll(checked) {
        if (checked) {
            this.state.entries.forEach(entry => {
                const entryId = entry.info_hash || entry.path;
                this.state.selectedEntries.add(entryId);
            });
        } else {
            this.state.selectedEntries.clear();
        }

        // Update all checkboxes
        document.querySelectorAll('.entry-checkbox').forEach(checkbox => {
            checkbox.checked = checked;
        });

        this.updateSelectionUI();
    }

    updateSelectionUI() {
        const selectedCount = this.state.selectedEntries.size;

        // Update count
        if (this.refs.selectedCount) {
            this.refs.selectedCount.textContent = selectedCount;
        }

        // Show/hide bulk actions bar
        if (this.refs.bulkActionsBar) {
            if (selectedCount > 0) {
                this.refs.bulkActionsBar.classList.remove('hidden');
            } else {
                this.refs.bulkActionsBar.classList.add('hidden');
            }
        }

        // Update select all checkbox state
        if (this.refs.selectAllCheckbox) {
            const allSelected = this.state.entries.length > 0 &&
                                this.state.entries.every(entry => {
                                    const entryId = entry.info_hash || entry.path;
                                    return this.state.selectedEntries.has(entryId);
                                });
            this.refs.selectAllCheckbox.checked = allSelected;
            this.refs.selectAllCheckbox.indeterminate = selectedCount > 0 && !allSelected;
        }
    }

    clearSelection() {
        this.state.selectedEntries.clear();
        document.querySelectorAll('.entry-checkbox').forEach(checkbox => {
            checkbox.checked = false;
        });
        this.updateSelectionUI();
    }

    bulkDownload() {
        const selectedEntries = this.getSelectedEntries();
        const files = selectedEntries.filter(e => !e.is_dir);

        if (files.length === 0) {
            window.createToast('No files selected for download', 'warning');
            return;
        }

        files.forEach(entry => {
            this.downloadFile(entry.path, entry.name);
        });

        window.createToast(`Downloading ${files.length} file(s)`, 'success');
    }

    async bulkMove() {
        const selectedEntries = this.getSelectedEntries();
        const torrents = selectedEntries.filter(e => e.can_delete && e.info_hash);

        if (torrents.length === 0) {
            window.createToast('No items selected for moving', 'warning');
            return;
        }

        if (torrents.length === 1) {
            this.showMoveModal(torrents[0].info_hash);
            return;
        }

        window.createToast('Bulk move not yet implemented for multiple items', 'info');
    }

    async bulkDelete() {
        const selectedEntries = this.getSelectedEntries();
        const torrents = selectedEntries.filter(e => e.can_delete && e.info_hash);

        if (torrents.length === 0) {
            window.createToast('No items selected for deletion', 'warning');
            return;
        }

        const names = torrents.map(t => t.name).join('\n');
        if (!confirm(`Delete ${torrents.length} torrent(s)?\n\n${names}\n\nThis will remove the items from the management system.`)) {
            return;
        }

        let successCount = 0;
        for (const torrent of torrents) {
            try {
                const response = await fetch(`${window.urlBase}api/browse/torrents/${torrent.info_hash}`, {
                    method: 'DELETE'
                });

                if (response.ok) {
                    successCount++;
                    this.state.selectedEntries.delete(torrent.info_hash);
                }
            } catch (error) {
                console.error(`Error deleting item ${torrent.name}:`, error);
            }
        }

        if (successCount > 0) {
            window.createToast(`Deleted ${successCount} of ${torrents.length} torrent(s)`, 'success');
            this.clearSelection();
            this.refresh();
        }
    }

    getSelectedEntries() {
        return this.state.entries.filter(entry => {
            const entryId = entry.info_hash || entry.path;
            return this.state.selectedEntries.has(entryId);
        });
    }
}
