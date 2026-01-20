# Mobile UX Redesign

## Goal
Create a mobile-first experience optimized for one-handed story browsing, similar to Instagram/Tinder feed patterns but showing multiple stories at once.

## Design Principles
1. **Maximum content space** - No persistent chrome/headers eating screen real estate
2. **One-handed operation** - Thumb-reachable tap zones, no swipe gestures required
3. **Efficient scrolling** - Virtual scrolling to handle large story lists without DOM bloat
4. **Quick actions** - Dismiss/save without opening menus
5. **Expandable content** - Tap to expand, scrollable within card

## UX Specification

### Story Cards
- Stories displayed as cards with clear visual separation
- Multiple cards visible at once (not one-at-a-time like Tinder)
- Each card shows: title, domain, score, time, teaser
- Tap zones at bottom of card (thumb-reachable):
  - Bottom-left area: Dismiss
  - Bottom-right area: Save for later
  - Card body tap: Expand content inline

### Expanded Content
- Tapping card body expands content inline
- Expanded area is scrollable (nested scroll)
- Tap again or scroll past to collapse
- Must work well on mobile (no desktop-style overflow issues)

### Menu Access
- Floating Action Button (FAB) in corner (bottom-right, thumb-reachable)
- Opens bottom sheet with:
  - View tabs (All / Read Later / Settings)
  - Filter toggles (Front page only, Show dismissed, etc.)
  - Status info
- Menu rarely used during browsing, stays out of way

### Infinite Scroll
- Doom scrolling with virtual list (DOM recycling)
- Only render visible cards + buffer above/below
- Reuse DOM elements instead of create/destroy (avoid GC churn)
- Smooth scrolling even with 1000+ stories

## Technical Implementation

### Virtual Scrolling
```
Approach: Fixed-height card estimation with dynamic adjustment

1. Estimate card height (collapsed vs expanded)
2. Calculate visible range based on scroll position
3. Render only visible cards + buffer (e.g., 5 above, 5 below)
4. Use transform: translateY() to position cards
5. Recycle DOM nodes: pool of ~20-30 card elements
6. On scroll: update which stories map to which DOM nodes
```

### DOM Structure (Mobile)
```html
<div class="mobile-feed">
  <!-- FAB menu trigger -->
  <button class="fab-menu">☰</button>

  <!-- Virtual scroll container -->
  <div class="card-viewport">
    <div class="card-spacer" style="height: {totalHeight}px">
      <!-- Only visible cards rendered -->
      <article class="story-card" style="transform: translateY({y}px)">
        <div class="card-content">
          <h2 class="card-title">...</h2>
          <div class="card-meta">...</div>
          <div class="card-teaser">...</div>
          <div class="card-expanded">...</div>
        </div>
        <div class="card-actions">
          <div class="action-zone dismiss" data-action="dismiss">×</div>
          <div class="action-zone save" data-action="save">★</div>
        </div>
      </article>
      <!-- More cards... -->
    </div>
  </div>

  <!-- Bottom sheet menu -->
  <div class="bottom-sheet hidden">
    <div class="sheet-handle"></div>
    <nav class="sheet-tabs">...</nav>
    <div class="sheet-filters">...</div>
  </div>
</div>
```

### Detection
```javascript
function isMobile() {
  return window.matchMedia('(max-width: 900px)').matches;
}

// Could also check touch capability:
function isTouchDevice() {
  return 'ontouchstart' in window || navigator.maxTouchPoints > 0;
}
```

### CSS Architecture
- Keep desktop styles as-is (sidebar layout)
- Mobile styles in `@media (max-width: 900px)` block
- Or: separate mobile.css loaded conditionally
- Use CSS custom properties for card dimensions

## Implementation Plan

### Phase 1: Basic Card Layout
- [x] Card CSS (mobile only, via media query)
- [x] Hide sidebar completely on mobile
- [x] Show FAB button
- [x] Basic card rendering (no virtual scroll yet)

### Phase 2: Tap Zones & Actions
- [x] Bottom tap zones on cards
- [x] Touch event handlers for dismiss/save
- [x] Visual feedback on tap (:active highlight)
- [x] Action confirmation (toast notifications)

### Phase 3: FAB & Bottom Sheet
- [x] FAB button styling and positioning
- [x] Bottom sheet component
- [x] View switching in bottom sheet
- [x] Filter controls in bottom sheet

### Phase 4: Expandable Content
- [x] Tap to expand card (via event delegation on story-main)
- [x] Nested scrollable area for content (overflow-y: auto, 60vh max)
- [x] Collapse on tap (tap again to collapse)
- [x] Smooth expand/collapse animation (CSS transition)
- [x] "Tap to read" indicator with fade gradient

### Phase 5: Virtual Scrolling
- [ ] Virtual scroll container
- [ ] Card height estimation
- [ ] Visible range calculation
- [ ] DOM node recycling pool
- [ ] Scroll position restoration
- [ ] Handle expanded cards (variable height)

### Phase 6: Polish
- [x] Loading states (spinning indicator)
- [x] Error handling (error message with retry hint)
- [x] Haptic feedback (via Vibration API)
- [x] Bottom sheet close methods (backdrop, handle, FAB)
- [x] Offline detection (banner + disabled refresh)
- [ ] Performance testing with 1000+ stories

## Open Questions
1. Card height: Fixed or variable? Variable is more complex for virtual scroll
2. Expand animation: CSS transition or JS-controlled?
3. Save DOM pool size: ~20 cards enough? Need testing
4. Scroll restoration: Save position per view?

## Files to Modify
- `frontend/style.css` - Mobile card styles in media query
- `frontend/app.js` - Mobile rendering, virtual scroll, touch handlers
- `frontend/index.html` - Minimal changes (FAB button, bottom sheet markup)

## References
- Virtual scroll concept: Reuse fixed pool of DOM elements
- Similar to: react-window, vue-virtual-scroller, vanilla implementations
- Bottom sheet pattern: Material Design, iOS action sheets
