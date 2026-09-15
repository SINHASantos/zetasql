/**
 * @fileoverview Manages draggable panel splitting and layout mode toggling
 * (auto/vertical).
 */

/**
 * Layout modes supported by SplitterManager.
 * @enum {string}
 */
const LayoutMode = {
  AUTO: 'auto',
  VERTICAL: 'vertical',
};

/**
 * Controller managing panel drag splitting and layout mode switching.
 */
class SplitterManager {
  /**
   * @param {!HTMLElement} splitter
   * @param {!HTMLElement} mainElement
   * @param {?HTMLElement} toggleButton
   * @param {function(): !Object<string, string>} getSettings Callback to
   *     retrieve all saved settings.
   * @param {function(string, string): undefined} updateSetting Callback to
   *     persist an updated setting value.
   */
  constructor(splitter, mainElement, toggleButton, getSettings, updateSetting) {
    /** @private @const {!HTMLElement} */
    this.splitter = splitter;
    /** @private @const {!HTMLElement} */
    this.mainElement = mainElement;
    /** @private @const {?HTMLElement} */
    this.toggleButton = toggleButton;
    /** @private @const {function(): !Object<string, string>} */
    this.getSettings = getSettings;
    /** @private @const {function(string, string): undefined} */
    this.updateSetting = updateSetting;
    /** @private {boolean} */
    this.isDragging = false;
    /** @private {!LayoutMode} */
    this.currentMode = LayoutMode.AUTO;

    // Maintain stable function references so event listeners can be detached
    // in removeEventListener when dragging stops.
    /** @private @const {function(!Event): undefined} */
    this.onMouseMoveBound = (e) => this.onMouseMove(e);
    /** @private @const {function(): undefined} */
    this.stopDraggingBound = () => this.stopDragging();

    this.splitter.addEventListener(
        'mousedown', (e) => void this.startDragging(e));
    this.toggleButton?.addEventListener(
        'click', () => void this.cycleLayoutMode());

    // Restore saved width percentage and layout mode preference.
    const settings = this.getSettings();
    const savedWidth = Number(settings['split_left_width']);
    if (!Number.isNaN(savedWidth) && savedWidth > 0) {
      this.setLeftWidthPercentage(Math.max(5, Math.min(95, savedWidth)));
    }

    const savedMode = settings['layout_mode'];
    if (savedMode === LayoutMode.AUTO || savedMode === LayoutMode.VERTICAL) {
      this.setLayoutMode(/** @type {!LayoutMode} */ (savedMode));
    } else {
      this.setLayoutMode(LayoutMode.AUTO);
    }
  }

  /**
   * Attaches mouse drag listeners and applies resizing styling.
   * @private
   * @param {!Event} e
   */
  startDragging(e) {
    e.preventDefault();
    this.isDragging = true;
    this.mainElement.classList.add('is-splitter-resizing');

    document.addEventListener('mousemove', this.onMouseMoveBound);
    document.addEventListener('mouseup', this.stopDraggingBound);
  }

  /**
   * Detaches mouse drag listeners and saves final width preference.
   * @private
   */
  stopDragging() {
    if (!this.isDragging) return;
    this.isDragging = false;
    this.mainElement.classList.remove('is-splitter-resizing');

    document.removeEventListener('mousemove', this.onMouseMoveBound);
    document.removeEventListener('mouseup', this.stopDraggingBound);

    const currentPct = this.getLeftWidthPercentage();
    this.updateSetting('split_left_width', currentPct.toFixed(2));
  }

  /**
   * Handles mouse drag motion to update split width percentage in the DOM.
   * @private
   * @param {!Event} e
   */
  onMouseMove(e) {
    if (!this.isDragging) return;
    const windowWidth = window.innerWidth;
    if (windowWidth <= 0) return;
    const mouseEvent = /** @type {!MouseEvent} */ (e);

    // Calculate mouse position percentage; pixel boundaries are enforced by CSS
    // Grid minmax().
    let pct = (mouseEvent.clientX / windowWidth) * 100;
    pct = Math.max(5, Math.min(95, pct));
    this.setLeftWidthPercentage(pct);
  }

  /**
   * Updates the CSS custom property on the main element.
   * @private
   * @param {number} pct
   */
  setLeftWidthPercentage(pct) {
    this.mainElement.style.setProperty('--left-width', `${pct}%`);
  }

  /**
   * Reads the currently applied left width percentage.
   * @private
   * @return {number}
   */
  getLeftWidthPercentage() {
    const val = this.mainElement.style.getPropertyValue('--left-width');
    const parsed = Number(val.replace('%', ''));
    return Number.isNaN(parsed) || parsed === 0 ? 40 : parsed;
  }

  /**
   * Applies layout mode (auto or vertical).
   * @private
   * @param {!LayoutMode} mode
   */
  setLayoutMode(mode) {
    this.currentMode = mode;
    this.mainElement.classList.remove('layout-vertical');

    if (mode === LayoutMode.VERTICAL) {
      this.mainElement.classList.add('layout-vertical');
    }

    this.updateSetting('layout_mode', mode);
  }

  /**
   * Gets the current layout mode.
   * @return {!LayoutMode}
   */
  getLayoutMode() {
    return this.currentMode;
  }

  /**
   * Toggles between auto responsive and vertical stacked modes.
   */
  cycleLayoutMode() {
    const nextMode = this.currentMode === LayoutMode.AUTO ?
        LayoutMode.VERTICAL :
        LayoutMode.AUTO;
    this.setLayoutMode(nextMode);
  }
}

exports.LayoutMode = LayoutMode;
exports.SplitterManager = SplitterManager;
