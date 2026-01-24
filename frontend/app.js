// =============================================================================
// State
// =============================================================================

let stories = [];
let selectedIndex = -1;
let currentView = 'all';
let statusPollInterval = null;

// Pagination state
let hasMore = false;
let nextCursor = null;  // Cursor for keyset pagination (format: "time:id")
let isLoadingMore = false;
const PAGE_SIZE = 50;
const MAX_DOM_ELEMENTS = 200; // Remove old elements when exceeding this

// =============================================================================
// Cached DOM elements (reduces repeated queries)
// =============================================================================

const dom = {
    storyList: null,
    readlaterList: null,
    storyCount: null,
    contentStatus: null,
    toast: null,
    escapeDiv: null, // Cached div for escapeHtml
    // Filter checkboxes
    showDismissed: null,
    showBlocked: null,
    frontPageOnly: null,
    sortOldest: null,
    // Other frequently accessed elements
    readlaterEmpty: null,
    shortcutsHelp: null,
    activityStats: null,
    // Mobile elements
    mobileStoryCount: null,
    mobileContentStatus: null,
    mobileActivityStats: null,
    // Other
    filterStatus: null,
    cfStatsValue: null,
    init() {
        this.storyList = document.getElementById('story-list');
        this.readlaterList = document.getElementById('readlater-list');
        this.storyCount = document.getElementById('story-count');
        this.contentStatus = document.getElementById('content-status');
        this.toast = document.getElementById('toast');
        this.escapeDiv = document.createElement('div');
        // Filter checkboxes
        this.showDismissed = document.getElementById('show-dismissed');
        this.showBlocked = document.getElementById('show-blocked');
        this.frontPageOnly = document.getElementById('front-page-only');
        this.sortOldest = document.getElementById('sort-oldest');
        // Other elements
        this.readlaterEmpty = document.getElementById('readlater-empty');
        this.shortcutsHelp = document.getElementById('shortcuts-help');
        this.activityStats = document.getElementById('activity-stats');
        // Mobile elements
        this.mobileStoryCount = document.getElementById('mobile-story-count');
        this.mobileContentStatus = document.getElementById('mobile-content-status');
        this.mobileActivityStats = document.getElementById('mobile-activity-stats');
        // Other
        this.filterStatus = document.getElementById('filter-status');
        this.cfStatsValue = document.getElementById('cf-stats-value');
    }
};

// =============================================================================
// Request Batching (for rapid mutations like dismiss/block)
// =============================================================================

const batcher = {
    queue: [],
    inflight: [],
    timeout: null,
    retryTimeout: null,
    waitingForOnline: false,
    DELAY: 150, // ms to wait before sending batch
    RETRY_DELAY: 2000, // ms to wait before retry
    MAX_RETRIES: 3,
    MAX_BATCH_SIZE: 50, // Split large queues into chunks
    STORAGE_KEY: 'hn_pending_batch',
    retryCount: 0,

    // Persist queue to localStorage for survival across page reloads
    _persist() {
        const pending = [...this.inflight, ...this.queue];
        if (pending.length > 0) {
            localStorage.setItem(this.STORAGE_KEY, JSON.stringify(pending));
        } else {
            localStorage.removeItem(this.STORAGE_KEY);
        }
    },

    // Load any pending items from previous session
    _restore() {
        try {
            const stored = localStorage.getItem(this.STORAGE_KEY);
            if (stored) {
                const items = JSON.parse(stored);
                if (Array.isArray(items) && items.length > 0) {
                    console.log(`Restored ${items.length} pending batch items from previous session`);
                    this.queue = items;
                    // Don't clear storage yet - wait for successful flush
                    // Schedule flush
                    if (!this.timeout) {
                        this.timeout = setTimeout(() => this.flush(), this.DELAY);
                    }
                }
            }
        } catch (e) {
            console.error('Failed to restore batch queue:', e);
            localStorage.removeItem(this.STORAGE_KEY);
        }
    },

    add(method, path) {
        this.queue.push({ method, path });
        this._persist();
        if (!this.timeout) {
            this.timeout = setTimeout(() => this.flush(), this.DELAY);
        }
    },

    flush(useBeacon = false) {
        if (this.timeout) {
            clearTimeout(this.timeout);
            this.timeout = null;
        }
        if (this.retryTimeout) {
            clearTimeout(this.retryTimeout);
            this.retryTimeout = null;
        }

        if (this.queue.length === 0 && this.inflight.length === 0) return;

        // If offline, wait for online event instead of burning retries
        if (!navigator.onLine && !useBeacon) {
            // Move queue to inflight to preserve order
            if (this.queue.length > 0) {
                this.inflight = [...this.inflight, ...this.queue];
                this.queue = [];
                this._persist();
            }
            if (!this.waitingForOnline) {
                this.waitingForOnline = true;
                console.log(`Offline - holding ${this.inflight.length} items until online`);
            }
            return;
        }
        this.waitingForOnline = false;

        // Combine queue with any failed inflight items being retried
        const allItems = [...this.inflight, ...this.queue];
        this.queue = [];

        // Split into batches if too large
        const batch = allItems.slice(0, this.MAX_BATCH_SIZE);
        this.inflight = batch; // Current batch being sent

        // Put overflow back in queue for next flush
        if (allItems.length > this.MAX_BATCH_SIZE) {
            this.queue = allItems.slice(this.MAX_BATCH_SIZE);
        }

        // Persist current state in case of crash/close during request
        this._persist();

        const body = JSON.stringify({ requests: batch });

        // Use sendBeacon for page unload (best-effort, can't retry)
        if (useBeacon && navigator.sendBeacon) {
            // For unload, try to send everything in one shot
            const allBody = JSON.stringify({ requests: [...batch, ...this.queue] });
            const sent = navigator.sendBeacon('/api/batch', new Blob([allBody], { type: 'application/json' }));
            if (sent) {
                // Beacon queued successfully - clear state and storage
                this.inflight = [];
                this.queue = [];
                this.retryCount = 0;
                localStorage.removeItem(this.STORAGE_KEY);
            }
            // If sendBeacon returns false (queue full), items remain in localStorage
            // and will be retried on next page load
            return;
        }

        fetch('/api/batch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body
        })
        .then(res => {
            if (res.ok) {
                // Success - clear inflight and update persistence
                this.inflight = [];
                this.retryCount = 0;
                this._persist(); // Clears storage if queue is also empty
                // If there's overflow in queue, schedule next batch
                if (this.queue.length > 0) {
                    this.timeout = setTimeout(() => this.flush(), 50);
                }
            } else if (res.status >= 400 && res.status < 500) {
                // Client error (4xx) - items are corrupt, drop them
                console.error('Batch rejected by server (4xx), dropping items:', this.inflight.length);
                this.inflight = [];
                this.retryCount = 0;
                this._persist();
            } else {
                // Server error (5xx) - will retry
                throw new Error(`HTTP ${res.status}`);
            }
        })
        .catch(e => {
            console.error('Batch request failed:', e);

            // If we went offline during request, wait for online
            if (!navigator.onLine) {
                this.waitingForOnline = true;
                console.log(`Went offline - holding ${this.inflight.length + this.queue.length} items`);
                return;
            }

            // Retry with backoff if under limit
            if (this.retryCount < this.MAX_RETRIES) {
                this.retryCount++;
                const delay = this.RETRY_DELAY * this.retryCount;
                console.log(`Retrying batch in ${delay}ms (attempt ${this.retryCount}/${this.MAX_RETRIES})`);
                this.retryTimeout = setTimeout(() => this.flush(), delay);
            } else {
                // Give up for now - items stay in localStorage for next session
                console.error('Batch failed after max retries, will retry on next page load:', this.inflight.length, 'items');
                // Don't clear inflight - _persist() already saved them
                // Just reset retry count so we don't block new items
                this.retryCount = 0;
            }
        });
    },

    // Called when coming back online
    onOnline() {
        if (this.waitingForOnline && (this.inflight.length > 0 || this.queue.length > 0)) {
            console.log(`Back online - flushing ${this.inflight.length + this.queue.length} held items`);
            this.retryCount = 0; // Fresh start
            this.flush();
        }
    },

    // For critical single requests (non-batched)
    async send(method, path) {
        const res = await fetch(path, { method });
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
    }
};

// =============================================================================
// Activity Stats (client-side tracking)
// =============================================================================

const activityStats = {
    STORAGE_KEY: 'hn_activity_stats',
    MAX_AGE_MS: 7 * 24 * 60 * 60 * 1000, // 7 days
    PRUNE_INTERVAL: 10, // Prune every N logs
    _logCount: 0,

    _load() {
        try {
            return JSON.parse(localStorage.getItem(this.STORAGE_KEY)) || [];
        } catch {
            return [];
        }
    },

    _save(events) {
        localStorage.setItem(this.STORAGE_KEY, JSON.stringify(events));
    },

    log(action) {
        const events = this._load();
        events.push({ action, ts: Date.now() });
        this._logCount++;
        // Prune old events periodically (not on every log)
        if (this._logCount >= this.PRUNE_INTERVAL) {
            this._logCount = 0;
            const cutoff = Date.now() - this.MAX_AGE_MS;
            const pruned = events.filter(e => e.ts > cutoff);
            this._save(pruned);
        } else {
            this._save(events);
        }
    },

    getCounts(action, sinceMs) {
        const events = this._load();
        const cutoff = Date.now() - sinceMs;
        return events.filter(e => e.action === action && e.ts > cutoff).length;
    },

    getStats() {
        const HOUR = 60 * 60 * 1000;
        const DAY = 24 * HOUR;
        const WEEK = 7 * DAY;

        return {
            dismissed: {
                hour: this.getCounts('dismiss', HOUR),
                today: this.getCounts('dismiss', DAY),
                week: this.getCounts('dismiss', WEEK)
            },
            saved: {
                hour: this.getCounts('save', HOUR),
                today: this.getCounts('save', DAY),
                week: this.getCounts('save', WEEK)
            },
            expanded: {
                hour: this.getCounts('expand', HOUR),
                today: this.getCounts('expand', DAY),
                week: this.getCounts('expand', WEEK)
            }
        };
    }
};

// =============================================================================
// Helpers
// =============================================================================

function getVisibleStoryElements() {
    const listEl = currentView === 'readlater' ? dom.readlaterList : dom.storyList;
    return Array.from(listEl.querySelectorAll('.story')).filter(el => el.style.display !== 'none');
}

// Fix selectedIndex to match the visually selected story's position
function syncSelectedIndex() {
    const visible = getVisibleStoryElements();
    const selectedEl = visible.find(el => el.classList.contains('selected'));
    if (selectedEl) {
        selectedIndex = visible.indexOf(selectedEl);
    } else {
        selectedIndex = visible.length > 0 ? 0 : -1;
    }
}

// =============================================================================
// API
// =============================================================================

const api = {
    async get(path) {
        const res = await fetch(path);
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
    },
    async post(path) {
        const res = await fetch(path, { method: 'POST' });
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
    },
    async delete(path) {
        const res = await fetch(path, { method: 'DELETE' });
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
    }
};

// =============================================================================
// Story Rendering
// =============================================================================

// Page render time (for consistent relative timestamps within a session)
// Refreshed when tab becomes visible again to keep times accurate
let pageRenderTime = Date.now();

function formatTime(timestamp) {
    const date = new Date(timestamp * 1000);
    const diff = (pageRenderTime - date.getTime()) / 1000;

    if (diff < 0) return 'just now';
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;

    const hours = Math.floor(diff / 3600);
    const days = Math.floor(hours / 24);

    if (days === 0) {
        return `${hours}h ago`;
    } else if (days < 7) {
        return `${days}d ago`;
    } else if (days < 30) {
        const weeks = Math.floor(days / 7);
        return `${weeks}w ago`;
    } else if (days < 365) {
        const months = Math.floor(days / 30);
        return `${months}mo ago`;
    }

    // For older than a year, show date
    return date.toLocaleDateString();
}

function renderStory(story) {
    const titleLower = story.title.toLowerCase();

    // Detect story type badges
    let badges = '';
    if (titleLower.startsWith('show hn')) badges += '<span class="badge badge-show">Show</span>';
    else if (titleLower.startsWith('ask hn')) badges += '<span class="badge badge-ask">Ask</span>';
    else if (titleLower.startsWith('tell hn')) badges += '<span class="badge badge-tell">Tell</span>';

    if (story.hit_front_page) badges += `<span class="badge badge-frontpage" title="Hit front page${story.front_page_rank ? ' (#' + story.front_page_rank + ')' : ''}">#${story.front_page_rank || 'FP'}</span>`;
    if (story.is_read_later) badges += '<span class="badge badge-readlater">Later</span>';
    if (story.content_status === 'blocked') badges += '<span class="badge badge-blocked" title="Content blocked - check manually">Blocked</span>';

    // Score indicator with tooltip explaining the breakdown
    let scoreHtml = '';
    if (story.net_score > 0 || story.net_score < 0) {
        const parts = [];
        if (story.domain_merit) parts.push(`domain +${story.domain_merit}`);
        if (story.word_merit) parts.push(`words +${story.word_merit}`);
        if (story.domain_demerit) parts.push(`domain -${story.domain_demerit}`);
        if (story.word_demerit) parts.push(`words -${story.word_demerit}`);
        const tooltip = parts.length > 0 ? parts.join(', ') : 'merit/demerit score';
        const cls = story.net_score > 0 ? 'positive' : 'negative';
        const sign = story.net_score > 0 ? '+' : '';
        scoreHtml = `<span class="score-indicator ${cls}" title="${tooltip}">${sign}${story.net_score}</span>`;
    }

    // Teaser - render as markdown (use safe renderer to prevent XSS)
    let teaserHtml = '';
    if (story.teaser) {
        const renderedTeaser = renderMarkdown(story.teaser);
        teaserHtml = `<div class="story-teaser" onclick="expandContent(${story.id})">${renderedTeaser}</div>`;
    } else if (story.content_status === 'pending' || story.content_status === 'fetching') {
        teaserHtml = `<div class="story-teaser loading">Loading content...</div>`;
    }

    const classes = ['story'];
    if (story.hit_front_page) classes.push('front-page');
    if (story.is_dismissed) classes.push('dismissed');
    if (story.is_read) classes.push('read');

    const link = story.url || `https://news.ycombinator.com/item?id=${story.id}`;
    const hnLink = `https://news.ycombinator.com/item?id=${story.id}`;

    return `
        <li class="${classes.join(' ')}" data-id="${story.id}">
            <div class="story-header">
                <div class="story-main">
                    <div class="story-title">
                        ${badges}
                        <a href="${escapeHtml(link)}" target="_blank" onclick="markOpened(${story.id})">${escapeHtml(story.title)}</a>
                        ${scoreHtml}
                    </div>
                    ${story.url ? `<div class="story-url"><a href="${escapeHtml(story.url)}" target="_blank" onclick="markOpened(${story.id})">${escapeHtml(story.url)}</a></div>` : ''}
                    <div class="story-meta">
                        ${story.score} pts |
                        <a href="${hnLink}" target="_blank">${story.descendants} comments</a> |
                        ${formatTime(story.time)} |
                        ${escapeHtml(story.by)}
                    </div>
                    ${teaserHtml}
                    <div class="story-content" id="content-${story.id}"></div>
                </div>
                <div class="story-actions">
                    <button class="action-btn" onclick="window.open('${escapeHtml(link)}', '_blank'); markOpened(${story.id})" title="Open in new tab (o)">↗</button>
                    <button class="action-btn readlater-btn ${story.is_read_later ? 'active' : ''}" onclick="toggleReadLater(${story.id})" title="Read later (r)">
                        ${story.is_read_later ? '★' : '☆'}
                    </button>
                    <button class="action-btn" onclick="expandContent(${story.id})" title="Expand (e)">◰</button>
                    <button class="action-btn danger" onclick="blockDomain('${escapeHtml(story.domain)}')" title="Block domain (b)">⊘</button>
                    ${story.is_dismissed
                        ? `<button class="action-btn restore" onclick="undismissStory(${story.id})" title="Restore (d)">↩</button>`
                        : `<button class="action-btn danger" onclick="dismissStory(${story.id})" title="Dismiss (d)">×</button>`}
                </div>
                <div class="mobile-actions">
                    ${story.is_dismissed
                        ? `<div class="action-zone restore" onclick="undismissStory(${story.id}); event.stopPropagation();">↩</div>`
                        : `<div class="action-zone dismiss" onclick="dismissStory(${story.id}); event.stopPropagation();">×</div>`}
                    <div class="action-zone save ${story.is_read_later ? 'active' : ''}" onclick="toggleReadLater(${story.id}); event.stopPropagation();">${story.is_read_later ? '★' : '☆'}</div>
                </div>
            </div>
        </li>
    `;
}

function renderStories(reset = true) {
    if (stories.length === 0) {
        dom.storyList.innerHTML = '<li class="empty-msg">No stories found</li>';
        updateStoryCount();
        selectedIndex = -1;
        return;
    }

    // Track selected story ID and expanded stories before re-render
    let selectedStoryId = null;
    const selectedEl = dom.storyList.querySelector('.story.selected');
    if (selectedEl) {
        selectedStoryId = parseInt(selectedEl.dataset.id);
    }

    const expandedIds = new Set();
    dom.storyList.querySelectorAll('.story-content.expanded').forEach(el => {
        const match = el.id.match(/content-(\d+)/);
        if (match) expandedIds.add(parseInt(match[1]));
    });

    // Filter stories (sorting is handled by backend)
    const frontPageOnly = dom.frontPageOnly.checked;

    let filteredStories = stories;
    if (frontPageOnly) {
        filteredStories = stories.filter(s => s.hit_front_page);
    }

    if (filteredStories.length === 0) {
        dom.storyList.innerHTML = '<li class="empty-msg">No stories match filters</li>';
        updateStoryCount();
        selectedIndex = -1;
        return;
    }

    // Get existing story IDs in DOM for append mode
    const existingDomIds = new Set();
    if (!reset) {
        dom.storyList.querySelectorAll('.story').forEach(el => {
            existingDomIds.add(parseInt(el.dataset.id));
        });
    }

    if (reset) {
        // Full re-render
        dom.storyList.innerHTML = filteredStories.map(renderStory).join('');
    } else {
        // Append only new stories
        const newStories = filteredStories.filter(s => !existingDomIds.has(s.id));
        if (newStories.length > 0) {
            // Remove loading indicator if present
            const loadingIndicator = dom.storyList.querySelector('.loading-more');
            if (loadingIndicator) loadingIndicator.remove();

            // Append new stories
            const fragment = document.createDocumentFragment();
            const temp = document.createElement('div');
            temp.innerHTML = newStories.map(renderStory).join('');
            while (temp.firstChild) {
                fragment.appendChild(temp.firstChild);
            }
            dom.storyList.appendChild(fragment);
        }
    }

    // Add loading indicator if more stories available
    updateLoadingIndicator();

    // Memory management: remove old DOM elements when list gets too long
    trimOldStories();

    updateStoryCount();

    // Restore expanded state
    for (const storyId of expandedIds) {
        const story = stories.find(s => s.id === storyId);
        const contentEl = document.getElementById(`content-${storyId}`);
        if (contentEl && story?.content) {
            contentEl.innerHTML = renderMarkdown(story.content);
            contentEl.classList.add('expanded');
            contentEl.dataset.loaded = 'true';
        }
    }

    // Restore selection by story ID (position may have changed due to sort)
    const visibleStories = getVisibleStoryElements();
    if (selectedStoryId != null) {
        const newIndex = visibleStories.findIndex(el => parseInt(el.dataset.id) === selectedStoryId);
        if (newIndex >= 0) {
            selectStory(newIndex);
        } else if (visibleStories.length > 0) {
            selectStory(0); // Fallback to first
        }
    } else if (visibleStories.length > 0) {
        // Initial load - select first story
        selectStory(0);
    }
}

function updateLoadingIndicator() {
    // Remove existing indicator
    const existing = dom.storyList.querySelector('.loading-more');
    if (existing) existing.remove();

    // Add indicator if more stories available
    if (hasMore && currentView === 'all') {
        const indicator = document.createElement('li');
        indicator.className = 'loading-more';
        indicator.innerHTML = '<span>Loading more stories...</span>';
        indicator.style.cssText = 'text-align: center; padding: 20px; color: var(--text-muted); display: none;';
        dom.storyList.appendChild(indicator);
    }

    // Re-observe for infinite scroll
    observeLoadingIndicator();
}

function trimOldStories() {
    const storyElements = dom.storyList.querySelectorAll('.story');
    if (storyElements.length <= MAX_DOM_ELEMENTS) return;

    // Remove from top of DOM (what user has scrolled past) to free memory
    const toRemove = storyElements.length - MAX_DOM_ELEMENTS;
    for (let i = 0; i < toRemove; i++) {
        const el = storyElements[i];
        // Don't remove selected story
        if (el.classList.contains('selected')) continue;
        el.remove();
    }

    // Adjust selectedIndex if needed
    syncSelectedIndex();
}

// =============================================================================
// Data Loading
// =============================================================================

async function loadStories(reset = true) {
    const dismissedOnly = dom.showDismissed.checked;
    const showBlocked = dom.showBlocked.checked;
    const sortOldest = dom.sortOldest.checked;
    const sort = sortOldest ? 'oldest' : 'newest';

    if (reset) {
        nextCursor = null;
        stories = [];
        hasMore = false; // Reset until we get fresh data from API
        // Show loading state only on initial load
        dom.storyList.innerHTML = '<li class="loading-msg">Loading stories...</li>';
    }

    try {
        let url = `/api/stories?dismissed_only=${dismissedOnly}&include_blocked=${showBlocked}&limit=${PAGE_SIZE}&sort=${sort}`;
        if (nextCursor) {
            url += `&cursor=${encodeURIComponent(nextCursor)}`;
        }
        const result = await api.get(url);

        if (reset) {
            stories = result.stories;
        } else {
            // Append new stories, avoiding duplicates
            const existingIds = new Set(stories.map(s => s.id));
            const newStories = result.stories.filter(s => !existingIds.has(s.id));
            stories = stories.concat(newStories);
        }

        hasMore = result.has_more;
        nextCursor = result.next_cursor;

        // renderStories() handles state preservation (selection, expanded content, scroll)
        renderStories(reset);
    } catch (e) {
        if (reset) {
            dom.storyList.innerHTML = '<li class="error-msg">Failed to load stories. Pull down to retry.</li>';
        }
        showToast('Failed to load stories: ' + e.message);
    }
}

async function loadMoreStories() {
    if (!hasMore || isLoadingMore) return;

    isLoadingMore = true;
    try {
        await loadStories(false);
    } finally {
        isLoadingMore = false;
    }
}

async function loadReadLater() {
    const dismissedOnly = dom.showDismissed.checked;
    const frontPageOnly = dom.frontPageOnly.checked;
    const sortOldest = dom.sortOldest.checked;
    const sort = sortOldest ? 'oldest' : 'newest';
    const empty = dom.readlaterEmpty;

    // Track current state before reload
    const selectedStoryId = getSelectedStory()?.id;
    const expandedIds = new Set();
    dom.readlaterList.querySelectorAll('.story-content.expanded').forEach(el => {
        const match = el.id.match(/content-(\d+)/);
        if (match) expandedIds.add(parseInt(match[1]));
    });

    try {
        // Read later list is typically small, load all at once
        const result = await api.get(`/api/readlater?dismissed_only=${dismissedOnly}&limit=500&sort=${sort}`);
        const items = result.stories;

        // Apply front page filter (sorting is done by backend)
        let filtered = frontPageOnly ? items.filter(s => s.hit_front_page) : items;

        // Store in stories array for keyboard navigation
        stories = filtered;

        if (filtered.length === 0) {
            dom.readlaterList.innerHTML = '';
            empty.style.display = 'block';
            selectedIndex = -1;
        } else {
            empty.style.display = 'none';
            dom.readlaterList.innerHTML = filtered.map(renderStory).join('');

            // Restore expanded state for stories that still exist
            for (const storyId of expandedIds) {
                const story = stories.find(s => s.id === storyId);
                const contentEl = document.getElementById(`content-${storyId}`);
                if (contentEl && story?.content) {
                    contentEl.innerHTML = renderMarkdown(story.content);
                    contentEl.classList.add('expanded');
                    contentEl.dataset.loaded = 'true';
                }
            }

            // Restore selection or select first
            if (selectedStoryId != null) {
                const visibleStories = getVisibleStoryElements();
                const newIndex = visibleStories.findIndex(el => parseInt(el.dataset.id) === selectedStoryId);
                if (newIndex >= 0) {
                    selectStory(newIndex);
                } else {
                    selectStory(0);
                }
            } else {
                selectStory(0);
            }
        }
    } catch (e) {
        showToast('Failed to load read later: ' + e.message);
    }
}

async function loadSettings() {
    try {
        const [blockedDomains, blockedWords, meritWords, demeritWords, meritDomains, demeritDomains] = await Promise.all([
            api.get('/api/blocked/domains'),
            api.get('/api/blocked/words'),
            api.get('/api/merit/words'),
            api.get('/api/demerit/words'),
            api.get('/api/merit/domains'),
            api.get('/api/demerit/domains'),
        ]);

        renderTags('blocked-domains', blockedDomains, d => removeBlockedDomain(d));
        renderTags('blocked-words', blockedWords, w => removeBlockedWord(w));
        renderTags('merit-words', meritWords.map(w => w.word), w => removeMeritWord(w));
        renderTags('demerit-words', demeritWords.map(w => w.word), w => removeDemeritWord(w));
        renderTags('merit-domains', meritDomains.map(d => d.domain), d => removeMeritDomain(d));
        renderTags('demerit-domains', demeritDomains.map(d => d.domain), d => removeDemeritDomain(d));

        // Load usage stats
        loadUsageStats();
    } catch (e) {
        showToast('Failed to load settings: ' + e.message);
    }
}

async function loadUsageStats() {
    const container = document.getElementById('usage-stats');
    try {
        const [usage, status] = await Promise.all([
            api.get('/api/usage'),
            api.get('/api/status'),
        ]);

        const formatMs = (ms) => {
            if (ms < 1000) return `${Math.round(ms)}ms`;
            if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
            return `${(ms / 60000).toFixed(1)}m`;
        };

        const formatDuration = (seconds) => {
            const hours = Math.floor(seconds / 3600);
            const mins = Math.floor((seconds % 3600) / 60);
            if (hours > 0) return `${hours}h ${mins}m`;
            return `${mins}m`;
        };

        let quotaHtml = '';
        if (status.cf_quota && status.cf_quota.exceeded) {
            const resetIn = formatDuration(status.cf_quota.resets_in_seconds);
            quotaHtml = `
                <div style="background: var(--warning-bg, #fff3cd); color: var(--warning-text, #856404); padding: 0.75rem; border-radius: 4px; margin-bottom: 1rem;">
                    <strong>Daily quota exceeded</strong><br>
                    Content extraction paused. Resets in <strong>${resetIn}</strong> (UTC midnight).
                </div>
            `;
        }

        container.innerHTML = `
            ${quotaHtml}
            <table style="width: 100%; font-size: 0.85rem;">
                <tr>
                    <td>Today:</td>
                    <td><strong>${usage.today.requests}</strong> requests</td>
                    <td><strong>${formatMs(usage.today.browser_ms)}</strong> browser time</td>
                </tr>
                <tr>
                    <td>This week:</td>
                    <td><strong>${usage.week.requests}</strong> requests</td>
                    <td><strong>${formatMs(usage.week.browser_ms)}</strong> browser time</td>
                </tr>
                <tr>
                    <td>This month:</td>
                    <td><strong>${usage.month.requests}</strong> requests</td>
                    <td><strong>${formatMs(usage.month.browser_ms)}</strong> browser time</td>
                </tr>
                <tr>
                    <td>All time:</td>
                    <td><strong>${usage.total.requests}</strong> requests</td>
                    <td><strong>${formatMs(usage.total.browser_ms)}</strong> browser time</td>
                </tr>
            </table>
            <p style="font-size: 0.75rem; color: var(--text-muted); margin-top: 0.5rem;">
                Timeout: ${status.cf_timeout_ms}ms | Workers: ${status.workers}
            </p>
        `;
    } catch (e) {
        container.innerHTML = '<span style="color: var(--danger);">Failed to load usage stats</span>';
    }
}

async function updateSidebarCfStats() {
    const el = dom.cfStatsValue;
    if (!el) return;

    try {
        const [usage, status] = await Promise.all([
            api.get('/api/usage'),
            api.get('/api/status'),
        ]);

        const formatMs = (ms) => {
            if (ms < 1000) return `${Math.round(ms)}ms`;
            if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
            return `${(ms / 60000).toFixed(1)}m`;
        };

        if (status.cf_quota && status.cf_quota.exceeded) {
            const mins = Math.floor(status.cf_quota.resets_in_seconds / 60);
            el.innerHTML = `<span class="cf-quota-warning">Quota exceeded, resets in ${mins}m</span>`;
        } else {
            el.textContent = `${usage.today.requests} req, ${formatMs(usage.today.browser_ms)}`;
        }
    } catch (e) {
        el.textContent = '--';
    }
}

// Store tag removal handlers for event delegation
const tagHandlers = {};

function renderTags(containerId, items, onRemove) {
    const container = document.getElementById(containerId);
    tagHandlers[containerId] = onRemove;

    if (items.length === 0) {
        container.innerHTML = '<span style="color: var(--fg-dim); font-size: 0.8rem;">None</span>';
        return;
    }
    container.innerHTML = items.map(item => `
        <span class="tag" data-value="${escapeHtml(item)}">
            ${escapeHtml(item)}
            <button type="button" aria-label="Remove">&times;</button>
        </span>
    `).join('');
}

// Event delegation for tag removal (set up once)
function initTagDelegation() {
    document.querySelectorAll('.tag-list').forEach(container => {
        container.addEventListener('click', (e) => {
            const btn = e.target.closest('button');
            if (!btn) return;
            const tag = btn.closest('.tag');
            if (!tag) return;
            const value = tag.dataset.value;
            const handler = tagHandlers[container.id];
            if (handler && value) {
                e.stopPropagation();
                handler(value);
            }
        });
    });
}

// =============================================================================
// Actions
// =============================================================================

async function toggleReadLater(storyId) {
    const story = stories.find(s => s.id === storyId);
    const el = document.querySelector(`.story[data-id="${storyId}"]`);
    // In readlater view, all visible stories are read later by definition
    const wasReadLater = currentView === 'readlater' ? true : story?.is_read_later;
    const wasSelected = el && el.classList.contains('selected');

    // Optimistic update
    if (story) {
        story.is_read_later = !story.is_read_later;
    }

    // Hide story from current view when it no longer belongs
    // - In 'all' view: hide when added to read later
    // - In 'readlater' view: hide when removed from read later (always, since wasReadLater=true)
    const nowReadLater = story ? story.is_read_later : !wasReadLater;
    const shouldHide = (currentView === 'all' && nowReadLater) ||
                       (currentView === 'readlater' && !nowReadLater);

    if (shouldHide && el) {
        el.style.display = 'none';

        // Get visible elements and handle selection/empty state
        const listEl = currentView === 'readlater' ? dom.readlaterList : dom.storyList;
        const visible = Array.from(listEl.querySelectorAll('.story')).filter(e => e.style.display !== 'none');

        if (visible.length === 0) {
            // Show empty state
            selectedIndex = -1;
            if (currentView === 'readlater') {
                dom.readlaterEmpty.style.display = 'block';
            }
        } else if (wasSelected) {
            // Reselect next story
            if (selectedIndex >= visible.length) {
                selectedIndex = Math.max(0, visible.length - 1);
            }
            selectStory(selectedIndex);
        } else {
            // Sync selectedIndex with visually selected story (may have shifted)
            syncSelectedIndex();
        }
        if (currentView === 'all') {
            updateStoryCount();
        }
    } else if (el) {
        // Just update buttons (only in all view since it stays visible there)
        const newState = story ? story.is_read_later : nowReadLater;
        const btn = el.querySelector('.action-btn.readlater-btn');
        if (btn) {
            btn.textContent = newState ? '★' : '☆';
            btn.classList.toggle('active', newState);
        }
        const mobileBtn = el.querySelector('.action-zone.save');
        if (mobileBtn) {
            mobileBtn.textContent = newState ? '★' : '☆';
            mobileBtn.classList.toggle('active', newState);
        }
    }

    // Background API call
    if (!wasReadLater) {
        batcher.add('POST', `/api/readlater/${storyId}`);
        showToast('Saved for later');
        activityStats.log('save');
        updateActivityStats();
    } else {
        batcher.add('DELETE', `/api/readlater/${storyId}`);
        showToast('Removed from read later');
    }
    hapticFeedback('light');
}

async function dismissStory(storyId) {
    // Check if we're dismissing the currently selected story
    const el = document.querySelector(`.story[data-id="${storyId}"]`);
    const wasDismissingSelected = el && el.classList.contains('selected');

    // Optimistic update - hide immediately (works for both main list and read later view)
    if (el) {
        el.style.display = 'none';
    }

    // Remove from local stories array (only applies in 'all' view)
    const idx = stories.findIndex(s => s.id === storyId);
    if (idx !== -1) stories.splice(idx, 1);

    // Get visible elements and handle selection/empty state
    const listEl = currentView === 'readlater' ? dom.readlaterList : dom.storyList;
    const visible = Array.from(listEl.querySelectorAll('.story')).filter(e => e.style.display !== 'none');

    if (visible.length === 0) {
        // Show empty state
        selectedIndex = -1;
        if (currentView === 'readlater') {
            dom.readlaterEmpty.style.display = 'block';
        }
    } else if (wasDismissingSelected) {
        // Reselect next story
        if (selectedIndex >= visible.length) {
            selectedIndex = Math.max(0, visible.length - 1);
        }
        selectStory(selectedIndex);
    } else {
        // Sync selectedIndex with visually selected story (may have shifted)
        syncSelectedIndex();
    }
    if (currentView === 'all') {
        updateStoryCount();
    }

    // Batched API call (fire-and-forget)
    batcher.add('POST', `/api/dismiss/${storyId}`);
    showToast('Dismissed');
    hapticFeedback('medium');
    activityStats.log('dismiss');
    updateActivityStats();
}

async function undismissStory(storyId) {
    // In "dismissed only" mode, undismissing should hide the story (it's no longer dismissed)
    const el = document.querySelector(`.story[data-id="${storyId}"]`);
    const wasSelected = el && el.classList.contains('selected');
    const dismissedOnly = dom.showDismissed.checked;

    // Optimistic update - hide immediately in "dismissed only" mode
    if (dismissedOnly && el) {
        el.style.display = 'none';

        // Get visible elements and handle selection/empty state
        const listEl = currentView === 'readlater' ? dom.readlaterList : dom.storyList;
        const visible = Array.from(listEl.querySelectorAll('.story')).filter(e => e.style.display !== 'none');

        if (visible.length === 0) {
            // Show empty state
            selectedIndex = -1;
            if (currentView === 'readlater') {
                dom.readlaterEmpty.style.display = 'block';
            }
        } else if (wasSelected) {
            // Reselect next story
            if (selectedIndex >= visible.length) {
                selectedIndex = Math.max(0, visible.length - 1);
            }
            selectStory(selectedIndex);
        } else {
            // Sync selectedIndex with visually selected story (may have shifted)
            syncSelectedIndex();
        }
        if (currentView === 'all') {
            updateStoryCount();
        }
    }

    // Update local state if story is in array
    const story = stories.find(s => s.id === storyId);
    if (story) {
        story.is_dismissed = false;
    }

    // API call
    batcher.add('DELETE', `/api/dismiss/${storyId}`);
    showToast('Restored');
    hapticFeedback('light');
}

async function blockDomain(domain) {
    if (!domain) return;

    // Optimistic update - hide all stories from this domain immediately
    const listEl = currentView === 'readlater' ? dom.readlaterList : dom.storyList;
    listEl.querySelectorAll('.story').forEach(el => {
        const story = stories.find(s => s.id === parseInt(el.dataset.id));
        if (story && story.domain === domain) {
            el.style.display = 'none';
        }
    });

    // Remove from local array
    stories = stories.filter(s => s.domain !== domain);

    // Handle selection/empty state
    const visible = getVisibleStoryElements();
    if (visible.length === 0) {
        selectedIndex = -1;
        if (currentView === 'readlater') {
            dom.readlaterEmpty.style.display = 'block';
        }
    } else {
        // Reselect - blocked domain may have included selected story
        syncSelectedIndex();
        selectStory(selectedIndex);
    }
    if (currentView === 'all') {
        updateStoryCount();
    }

    // Batched API call (fire-and-forget)
    batcher.add('POST', `/api/blocked/domains?domain=${encodeURIComponent(domain)}`);
    showToast(`Blocked: ${domain}`);
}

function updateStoryCount() {
    const visible = getVisibleStoryElements();
    dom.storyCount.textContent = `${visible.length} stories`;
    updateMobileStatus();
}

function updateFilterStatus() {
    if (!dom.filterStatus) return;
    const filters = [];
    if (dom.showDismissed.checked) filters.push('dismissed');
    if (dom.showBlocked.checked) filters.push('blocked');
    if (dom.frontPageOnly.checked) filters.push('front page');

    const sort = dom.sortOldest.checked ? 'oldest first' : 'newest first';
    const filterText = filters.length > 0 ? filters.join(', ') : 'all';

    dom.filterStatus.textContent = `${filterText} · ${sort}`;
}

function updateActivityStats() {
    const stats = activityStats.getStats();
    const el = dom.activityStats;
    if (el) {
        el.innerHTML = `
            <div class="stat-row"><span>Dismissed:</span> <span>${stats.dismissed.hour}h / ${stats.dismissed.today}d / ${stats.dismissed.week}w</span></div>
            <div class="stat-row"><span>Saved:</span> <span>${stats.saved.hour}h / ${stats.saved.today}d / ${stats.saved.week}w</span></div>
            <div class="stat-row"><span>Expanded:</span> <span>${stats.expanded.hour}h / ${stats.expanded.today}d / ${stats.expanded.week}w</span></div>
        `;
    }
    // Update mobile too
    const mobileEl = dom.mobileActivityStats;
    if (mobileEl) {
        mobileEl.innerHTML = el ? el.innerHTML : '';
    }
}

async function markOpened(storyId) {
    try {
        await api.post(`/api/story/${storyId}/opened`);
    } catch (e) {
        console.error('Failed to mark opened:', e);
    }
}

async function expandContent(storyId, { skipLog = false } = {}) {
    const contentEl = document.getElementById(`content-${storyId}`);
    if (!contentEl) return;

    // Toggle if already expanded
    if (contentEl.classList.contains('expanded')) {
        contentEl.classList.remove('expanded');
        contentEl.removeAttribute('tabindex');
        return;
    }

    // Log expand action (skip for auto-expand during navigation)
    if (!skipLog) {
        activityStats.log('expand');
        updateActivityStats();
    }

    // Make focusable for keyboard scrolling
    contentEl.setAttribute('tabindex', '-1');

    // Show immediately with cached content if available
    const story = stories.find(s => s.id === storyId);
    if (story && story.content) {
        contentEl.innerHTML = renderMarkdown(story.content);
        contentEl.classList.add('expanded');
        contentEl.dataset.loaded = 'true';
        contentEl.focus();
        return;
    }

    // Load content if not already loaded or loading
    if (!contentEl.dataset.loaded && !contentEl.dataset.loading) {
        contentEl.dataset.loading = 'true';
        contentEl.innerHTML = 'Loading...';
        contentEl.classList.add('expanded');
        contentEl.focus();

        try {
            const data = await api.get(`/api/story/${storyId}/content`);
            if (data.content) {
                contentEl.innerHTML = renderMarkdown(data.content);
                // Cache it
                if (story) story.content = data.content;
            } else if (data.status === 'failed') {
                contentEl.innerHTML = '<em>Failed to load content</em>';
            } else if (data.status === 'blocked') {
                contentEl.innerHTML = '<em>Content blocked by site - check manually</em>';
            } else {
                contentEl.innerHTML = '<em>Content not yet available</em>';
            }
            contentEl.dataset.loaded = 'true';
        } catch (e) {
            contentEl.innerHTML = '<em>Error loading content</em>';
        } finally {
            delete contentEl.dataset.loading;
        }
    } else if (!contentEl.dataset.loading) {
        contentEl.classList.add('expanded');
        contentEl.focus();
    }
}

async function fetchNewStories() {
    try {
        showToast('Fetching new stories...');
        const result = await api.post('/api/fetch');
        showToast(`Fetched ${result.fetched} new stories`);
        // Reload current view to preserve context
        if (currentView === 'readlater') {
            await loadReadLater();
        } else {
            await loadStories();
        }
    } catch (e) {
        showToast('Fetch failed: ' + e.message);
    }
}

// Settings actions
async function addBlockedDomain() {
    const input = document.getElementById('add-blocked-domain');
    const domain = input.value.trim();
    if (!domain) return;
    try {
        await api.post(`/api/blocked/domains?domain=${encodeURIComponent(domain)}`);
        input.value = '';
        await loadSettings();
        showToast(`Blocked: ${domain}`);
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function removeBlockedDomain(domain) {
    try {
        await api.delete(`/api/blocked/domains?domain=${encodeURIComponent(domain)}`);
        await loadSettings();
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function addBlockedWord() {
    const input = document.getElementById('add-blocked-word');
    const word = input.value.trim();
    if (!word) return;
    try {
        await api.post(`/api/blocked/words?word=${encodeURIComponent(word)}`);
        input.value = '';
        await loadSettings();
        showToast(`Blocked word: ${word}`);
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function removeBlockedWord(word) {
    try {
        await api.delete(`/api/blocked/words?word=${encodeURIComponent(word)}`);
        await loadSettings();
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function addMeritWord() {
    const input = document.getElementById('add-merit-word');
    const word = input.value.trim();
    if (!word) return;
    try {
        await api.post(`/api/merit/words?word=${encodeURIComponent(word)}`);
        input.value = '';
        await loadSettings();
        showToast(`Added merit word: ${word}`);
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function removeMeritWord(word) {
    try {
        await api.delete(`/api/merit/words?word=${encodeURIComponent(word)}`);
        await loadSettings();
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function addDemeritWord() {
    const input = document.getElementById('add-demerit-word');
    const word = input.value.trim();
    if (!word) return;
    try {
        await api.post(`/api/demerit/words?word=${encodeURIComponent(word)}`);
        input.value = '';
        await loadSettings();
        showToast(`Added demerit word: ${word}`);
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function removeDemeritWord(word) {
    try {
        await api.delete(`/api/demerit/words?word=${encodeURIComponent(word)}`);
        await loadSettings();
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function addMeritDomain() {
    const input = document.getElementById('add-merit-domain');
    const domain = input.value.trim();
    if (!domain) return;
    try {
        await api.post(`/api/merit/domains?domain=${encodeURIComponent(domain)}`);
        input.value = '';
        await loadSettings();
        showToast(`Added merit domain: ${domain}`);
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function removeMeritDomain(domain) {
    try {
        await api.delete(`/api/merit/domains?domain=${encodeURIComponent(domain)}`);
        await loadSettings();
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function addDemeritDomain() {
    const input = document.getElementById('add-demerit-domain');
    const domain = input.value.trim();
    if (!domain) return;
    try {
        await api.post(`/api/demerit/domains?domain=${encodeURIComponent(domain)}`);
        input.value = '';
        await loadSettings();
        showToast(`Added demerit domain: ${domain}`);
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

async function removeDemeritDomain(domain) {
    try {
        await api.delete(`/api/demerit/domains?domain=${encodeURIComponent(domain)}`);
        await loadSettings();
    } catch (e) {
        showToast('Error: ' + e.message);
    }
}

// =============================================================================
// Navigation
// =============================================================================

function selectStory(index) {
    const visibleStories = getVisibleStoryElements();
    if (index < 0 || index >= visibleStories.length) return;

    // Collapse previously selected story's content
    if (selectedIndex >= 0 && selectedIndex < visibleStories.length) {
        const prevEl = visibleStories[selectedIndex];
        const prevContent = prevEl.querySelector('.story-content');
        if (prevContent) {
            prevContent.classList.remove('expanded');
            prevContent.removeAttribute('tabindex');
        }
    }

    selectedIndex = index;

    // Update visual selection
    visibleStories.forEach((el, i) => {
        el.classList.toggle('selected', i === index);
    });

    // Scroll into view
    const selected = visibleStories[index];
    if (selected) {
        selected.scrollIntoView({ behavior: 'instant', block: 'nearest' });

        // Auto-expand content for selected story (don't log - it's navigation, not manual expand)
        const storyId = parseInt(selected.dataset.id);
        if (storyId) {
            expandContent(storyId, { skipLog: true });
        }
    }
}

function getSelectedStory() {
    const visibleStories = getVisibleStoryElements();
    if (selectedIndex < 0 || selectedIndex >= visibleStories.length) return null;
    const el = visibleStories[selectedIndex];
    const storyId = parseInt(el.dataset.id);
    return stories.find(s => s.id === storyId);
}

function openSelectedStory() {
    const story = getSelectedStory();
    if (!story) return;
    const link = story.url || `https://news.ycombinator.com/item?id=${story.id}`;
    window.open(link, '_blank');
    markOpened(story.id);
}

function openSelectedComments() {
    const story = getSelectedStory();
    if (!story) return;
    window.open(`https://news.ycombinator.com/item?id=${story.id}`, '_blank');
}

// =============================================================================
// Views
// =============================================================================

function switchView(view) {
    currentView = view;

    // Update tabs
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.view === view);
    });

    // Update views
    document.querySelectorAll('.view').forEach(v => {
        v.classList.toggle('active', v.id === `view-${view}`);
    });

    // Load data for view
    if (view === 'all') {
        loadStories();
    } else if (view === 'readlater') {
        loadReadLater();
    } else if (view === 'settings') {
        loadSettings();
    }
}

// =============================================================================
// Status
// =============================================================================

function stopStatusPolling() {
    if (statusPollInterval) {
        clearInterval(statusPollInterval);
        statusPollInterval = null;
    }
}

function startStatusPolling() {
    stopStatusPolling(); // Clear any existing interval

    // Track last known content counts to detect changes
    let lastPending = -1;
    let lastDone = -1;

    statusPollInterval = setInterval(async () => {
        try {
            const stats = await api.get('/api/stats');

            // Update status bar
            dom.storyCount.textContent = `${getVisibleStoryElements().length} stories`;
            let contentStatus = `Content: ${stats.done_content}/${stats.total_stories} done`;
            if (stats.pending_content > 0) contentStatus += `, ${stats.pending_content} pending`;
            if (stats.blocked_content > 0) contentStatus += `, ${stats.blocked_content} blocked`;
            dom.contentStatus.textContent = contentStatus;
            updateMobileStatus();

            // Update sidebar CF stats
            updateSidebarCfStats();

            // Only fetch story updates if content status actually changed
            const contentChanged = stats.pending_content !== lastPending || stats.done_content !== lastDone;
            lastPending = stats.pending_content;
            lastDone = stats.done_content;

            if (contentChanged && (currentView === 'all' || currentView === 'readlater')) {
                // Fetch only stories with updated content
                const updates = await api.get('/api/stories/updates');

                // Update existing stories in-place - no DOM rebuild
                // New stories (from background fetch) are ignored here;
                // user will see them on manual refresh or view switch
                for (const update of updates) {
                    const existing = stories.find(s => s.id === update.id);
                    if (existing) {
                        existing.teaser = update.teaser;
                        existing.content_status = update.content_status;
                        existing.content = update.content;

                        // Update teaser in DOM
                        const storyEl = document.querySelector(`.story[data-id="${update.id}"]`);
                        const teaserEl = storyEl?.querySelector('.story-teaser');
                        if (teaserEl && update.teaser) {
                            teaserEl.innerHTML = renderMarkdown(update.teaser);
                            teaserEl.classList.remove('loading');
                        }
                    }
                }
            }
        } catch (e) {
            console.error('Status poll error:', e);
        }
    }, 5000);
}

// =============================================================================
// UI Helpers
// =============================================================================

function escapeHtml(text) {
    if (!text) return '';
    dom.escapeDiv.textContent = text;
    return dom.escapeDiv.innerHTML;
}

function renderMarkdown(text) {
    if (!text) return '';

    // Extract and protect links first (before escaping)
    const links = [];
    let processed = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (match, linkText, url) => {
        const idx = links.length;
        links.push({ text: linkText, url: url });
        return `\x00LINK${idx}\x00`;
    });

    // Now escape HTML
    let html = escapeHtml(processed);

    // Restore links with proper HTML
    html = html.replace(/\x00LINK(\d+)\x00/g, (match, idx) => {
        const link = links[parseInt(idx)];
        return `<a href="${escapeHtml(link.url)}" target="_blank" rel="noopener">${escapeHtml(link.text)}</a>`;
    });

    // Headers
    html = html.replace(/^### (.+)$/gm, '<h4>$1</h4>');
    html = html.replace(/^## (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^# (.+)$/gm, '<h2>$1</h2>');

    // Bold and italic
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // Lists
    html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
    html = html.replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>');

    // Paragraphs (double newlines)
    html = html.replace(/\n\n/g, '</p><p>');
    html = '<p>' + html + '</p>';
    html = html.replace(/<p><\/p>/g, '');

    // Single newlines to breaks
    html = html.replace(/\n/g, '<br>');

    return html;
}

let toastTimeout = null;
function showToast(message) {
    if (toastTimeout) clearTimeout(toastTimeout);
    dom.toast.textContent = message;
    dom.toast.classList.add('visible');
    toastTimeout = setTimeout(() => dom.toast.classList.remove('visible'), 2500);
}

// =============================================================================
// Keyboard Shortcuts
// =============================================================================

document.addEventListener('keydown', (e) => {
    // Ignore if typing in input
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

    // Story-related shortcuts only work in All and Read Later views
    const inStoryView = currentView === 'all' || currentView === 'readlater';
    const story = inStoryView ? getSelectedStory() : null;

    switch (e.key) {
        case 'j':
            if (inStoryView) selectStory(selectedIndex - 1);  // j = up visually
            break;
        case 'k':
            if (inStoryView) selectStory(selectedIndex + 1);  // k = down visually
            break;
        case 'o':
            if (inStoryView) openSelectedStory();
            break;
        case 'Enter':
            if (inStoryView) openSelectedComments();
            break;
        case 'e':
            if (story) expandContent(story.id);
            break;
        case 'r':
            if (story) toggleReadLater(story.id);
            break;
        case 'd':
            if (story) {
                story.is_dismissed ? undismissStory(story.id) : dismissStory(story.id);
            }
            break;
        case 'b':
            if (story && story.domain) blockDomain(story.domain);
            break;
        case 'f':
            if (inStoryView) fetchNewStories();
            break;
        case '1':
            switchView('all');
            break;
        case '2':
            switchView('readlater');
            break;
        case '3':
            switchView('settings');
            break;
        case '?':
            dom.shortcutsHelp.classList.toggle('hidden');
            break;
        case 'Escape':
            dom.shortcutsHelp.classList.add('hidden');
            break;
    }
});

// =============================================================================
// Event Listeners
// =============================================================================

// Tab clicks
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => switchView(tab.dataset.view));
});

// Filter checkboxes - most filters affect both views
function reloadCurrentView() {
    if (currentView === 'readlater') {
        loadReadLater();
    } else {
        loadStories();
    }
}

dom.showDismissed.addEventListener('change', () => {
    updateFilterStatus();
    reloadCurrentView();
});
dom.showBlocked.addEventListener('change', () => {
    updateFilterStatus();
    loadStories(); // Only affects All view
});
dom.frontPageOnly.addEventListener('change', () => {
    updateFilterStatus();
    // In All view, just re-render (data already loaded). In Read Later, reload.
    if (currentView === 'readlater') {
        loadReadLater();
    } else {
        renderStories();
    }
});
dom.sortOldest.addEventListener('change', () => {
    updateFilterStatus();
    // Sort is now handled by backend, so reload data
    reloadCurrentView();
});

// Refresh button
document.getElementById('btn-refresh').addEventListener('click', fetchNewStories);

// Enter key in add forms
document.querySelectorAll('.add-form input').forEach(input => {
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.target.nextElementSibling.click();
        }
    });
});

// =============================================================================
// Theme
// =============================================================================

function initTheme() {
    const saved = localStorage.getItem('theme');
    if (saved) {
        document.documentElement.setAttribute('data-theme', saved);
    }
    // If no saved preference, CSS handles system preference via media query
}

function toggleTheme() {
    const current = document.documentElement.getAttribute('data-theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    // Determine current effective theme
    let isDark;
    if (current === 'dark') {
        isDark = true;
    } else if (current === 'light') {
        isDark = false;
    } else {
        // No override, using system preference
        isDark = prefersDark;
    }

    // Toggle to opposite
    const newTheme = isDark ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
}

// =============================================================================
// Mobile UI
// =============================================================================

function isMobile() {
    return window.matchMedia('(max-width: 900px)').matches;
}

// Light haptic feedback for mobile actions
function hapticFeedback(type = 'light') {
    if (!navigator.vibrate) return;
    switch (type) {
        case 'light': navigator.vibrate(10); break;
        case 'medium': navigator.vibrate(20); break;
        case 'success': navigator.vibrate([10, 50, 10]); break;
    }
}

// Mobile card tap handler - expand content when tapping card body (not links/actions)
function initMobileCardTap() {
    const handler = (e) => {
        if (!isMobile()) return;

        // Ignore clicks on links, buttons, action zones, and teaser (teaser has its own onclick)
        if (e.target.closest('a, button, .action-zone, .mobile-actions, .story-teaser')) return;

        // Find the story card
        const story = e.target.closest('.story');
        if (!story) return;

        const storyId = parseInt(story.dataset.id);
        if (storyId) {
            expandContent(storyId);
        }
    };
    // Listen on both story lists
    dom.storyList.addEventListener('click', handler);
    dom.readlaterList.addEventListener('click', handler);
}

function toggleBottomSheet() {
    const sheet = document.querySelector('.bottom-sheet');
    const backdrop = document.querySelector('.bottom-sheet-backdrop');
    if (!sheet || !backdrop) return;

    const isOpen = sheet.classList.contains('open');
    sheet.classList.toggle('open', !isOpen);
    backdrop.classList.toggle('open', !isOpen);

    // Sync mobile filters with desktop state when opening
    if (!isOpen) {
        syncMobileFiltersFromDesktop();
    }
}

function syncMobileFiltersFromDesktop() {
    const filters = ['show-dismissed', 'show-blocked', 'front-page-only', 'sort-oldest'];
    filters.forEach(id => {
        const desktopEl = document.getElementById(id);
        const mobileEl = document.getElementById('mobile-' + id);
        if (desktopEl && mobileEl) {
            mobileEl.checked = desktopEl.checked;
        }
    });
}

function syncFilter(id, checked) {
    const desktopEl = document.getElementById(id);
    if (desktopEl) {
        desktopEl.checked = checked;
        // Trigger change event to run the filter logic
        desktopEl.dispatchEvent(new Event('change'));
    }
}

function updateMobileStatus() {
    const mobileStoryCount = dom.mobileStoryCount;
    const mobileContentStatus = dom.mobileContentStatus;

    if (mobileStoryCount && dom.storyCount) {
        mobileStoryCount.textContent = dom.storyCount.textContent;
    }
    if (mobileContentStatus && dom.contentStatus) {
        mobileContentStatus.textContent = dom.contentStatus.textContent;
    }
}

// =============================================================================
// Offline Detection
// =============================================================================

function updateOnlineStatus() {
    const isOffline = !navigator.onLine;
    document.body.classList.toggle('offline', isOffline);
    if (isOffline) {
        showToast('You are offline');
    } else {
        showToast('Back online');
        // Flush any held batch requests
        batcher.onOnline();
        // Refresh data when coming back online
        if (currentView === 'all') {
            loadStories();
        } else if (currentView === 'readlater') {
            loadReadLater();
        }
    }
}

window.addEventListener('online', updateOnlineStatus);
window.addEventListener('offline', updateOnlineStatus);

// =============================================================================
// Infinite Scroll
// =============================================================================

let infiniteScrollObserver = null;
let observedIndicator = null;

function initInfiniteScroll() {
    // Use Intersection Observer for efficient scroll detection
    infiniteScrollObserver = new IntersectionObserver((entries) => {
        const entry = entries[0];
        if (entry && entry.isIntersecting && hasMore && !isLoadingMore && currentView === 'all') {
            // Show loading indicator
            const indicator = dom.storyList.querySelector('.loading-more');
            if (indicator) indicator.style.display = 'block';

            loadMoreStories();
        }
    }, {
        root: null, // viewport
        rootMargin: '200px', // Load more when within 200px of viewport
        threshold: 0
    });
}

function observeLoadingIndicator() {
    if (!infiniteScrollObserver) return;

    const indicator = dom.storyList.querySelector('.loading-more');

    // Unobserve previous indicator if it's different (element was recreated)
    if (observedIndicator && observedIndicator !== indicator) {
        infiniteScrollObserver.unobserve(observedIndicator);
        observedIndicator = null;
    }

    // Observe new indicator
    if (indicator && indicator !== observedIndicator) {
        infiniteScrollObserver.observe(indicator);
        observedIndicator = indicator;
    }
}

// =============================================================================
// Initialize
// =============================================================================

initTheme();
dom.init();
batcher._restore(); // Restore any pending actions from previous session
initTagDelegation();
initMobileCardTap();
initInfiniteScroll();

// Handle PWA shortcut URLs (e.g., /?view=readlater)
const urlParams = new URLSearchParams(window.location.search);
const initialView = urlParams.get('view');
if (initialView && ['all', 'readlater', 'settings'].includes(initialView)) {
    switchView(initialView);
} else {
    loadStories();
}

startStatusPolling();
updateSidebarCfStats();
updateActivityStats();
updateFilterStatus();

// Check initial offline state (without toast)
if (!navigator.onLine) {
    document.body.classList.add('offline');
}

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    stopStatusPolling();
    batcher.flush(true); // Use sendBeacon for guaranteed delivery
    if (infiniteScrollObserver) {
        infiniteScrollObserver.disconnect();
    }
});

// Also flush on visibility change (tab switch/close)
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
        batcher.flush(true); // Use sendBeacon for guaranteed delivery
    } else if (document.visibilityState === 'visible') {
        // Refresh pageRenderTime when tab becomes visible for accurate relative times
        pageRenderTime = Date.now();
    }
});
