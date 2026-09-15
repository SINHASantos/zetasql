/**
 * @fileoverview Manages theme (light/dark mode) toggling and persistence.
 */

/**
 * Controller managing light/dark theme toggling and persistence.
 */
class ThemeManager {
  /**
   * @param {!HTMLElement} toggleButton
   * @param {function(): !Object<string, string>} getSettings Callback to
   *     retrieve all saved settings.
   * @param {function(string, string): undefined} updateSetting Callback to
   *     persist updated setting value.
   */
  constructor(toggleButton, getSettings, updateSetting) {
    /** @private @const {!HTMLElement} */
    this.toggleButton = toggleButton;
    /** @private @const {function(): !Object<string, string>} */
    this.getSettings = getSettings;
    /** @private @const {function(string, string): undefined} */
    this.updateSetting = updateSetting;

    this.toggleButton.addEventListener('click', () => {
      this.toggleTheme();
    });

    this.applyTheme(this.getSavedTheme());
    this.updateButtonText(
        document.documentElement.classList.contains('dark-mode'));
  }

  /**
   * Toggles dark mode state, persists preference, and updates button UI.
   * @private
   */
  toggleTheme() {
    const isDarkMode = document.documentElement.classList.toggle('dark-mode');
    this.updateSetting('theme', isDarkMode ? 'dark' : 'light');
    this.updateButtonText(isDarkMode);
  }

  /**
   * Reads saved theme or checks system preference.
   * @private
   * @return {string}
   */
  getSavedTheme() {
    const theme = this.getSettings()['theme'];
    if (theme) return theme;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' :
                                                                       'light';
  }

  /**
   * Applies the given theme class to the document element.
   * @private
   * @param {string} theme
   */
  applyTheme(theme) {
    document.documentElement.classList.toggle('dark-mode', theme === 'dark');
  }

  /**
   * Updates button accessible label and tooltip.
   * @private
   * @param {boolean} isDarkMode
   */
  updateButtonText(isDarkMode) {
    this.toggleButton.setAttribute(
        'title', isDarkMode ? 'Switch to Light Mode' : 'Switch to Dark Mode');
  }
}

exports.ThemeManager = ThemeManager;
