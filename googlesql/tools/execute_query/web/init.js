/**
 * @fileoverview Main entry point initializing UI components for Execute Query
 * web UI.
 */
/**
 * Initializes web UI components including theme manager and draggable splitter.
 */
function init() {
  const darkModeToggle =
      /** @type {?HTMLElement} */ (document.getElementById('dark-mode-toggle'));
  if (darkModeToggle) {
    new ThemeManager(darkModeToggle, getSettings, updateSetting);
  }

  const splitter =
      /** @type {?HTMLElement} */ (document.getElementById('splitter'));
  const main = /** @type {?HTMLElement} */ (document.querySelector('main'));
  const layoutToggle =
      /** @type {?HTMLElement} */ (document.getElementById('layout-toggle'));
  if (splitter && main) {
    new SplitterManager(
        splitter, main, layoutToggle, getSettings, updateSetting);
  }
}

if (typeof document !== 'undefined') {
  init();
}
