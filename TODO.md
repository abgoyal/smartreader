# TODO

## Mobile: Nested Scroll Behavior

**Problem:** When expanded content area is scrollable, scrolling can get mixed up between the inner content scroll and the outer feed scroll. User sometimes scrolls the feed when they meant to scroll content, or vice versa.

**Context:**
- Feed (story list) is scrollable
- Expanded content has `max-height: 60vh` and `overflow-y: auto`
- When inner scroll reaches boundary, it chains to outer scroll

**Options considered:**

1. **`overscroll-behavior: contain`** (CSS) - Recommended first try
   - Prevents scroll chaining at boundaries
   - One-line fix: add to `.story-content.expanded`
   - Well supported on modern browsers

2. **Lock feed scroll when expanded** (JS)
   - Add `overflow: hidden` to feed when content expanded
   - More restrictive but clearer
   - Requires JS to toggle

3. **Full-screen modal** - Changes UX significantly

4. **Touch event interception** - Complex, usually overkill

**Next step:** Try Option 1 first, test, escalate to Option 2 if needed.
