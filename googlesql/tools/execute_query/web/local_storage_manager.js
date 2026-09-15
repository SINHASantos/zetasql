/**
 * @fileoverview Manages application settings stored in local persistent
 * storage.
 */

/** @const {string} */
const STORAGE_KEY = 'execute-query-settings';

/**
 * Retrieves and parses all application settings from localStorage.
 * @return {!Object<string, string>}
 */
function getSettings() {
  try {
    const item = globalThis.localStorage?.getItem(STORAGE_KEY);
    if (!item) return {};
    const parsed = JSON.parse(item);
    if (parsed && typeof parsed === 'object') {
      return /** @type {!Object<string, string>} */ (parsed);
    }
    return {};
  } catch {
    return {};
  }
}

/**
 * Updates a setting value by key and serializes back to localStorage.
 * @param {string} key
 * @param {string} value
 */
function updateSetting(key, value) {
  const settings = getSettings();
  settings[key] = value;
  try {
    globalThis.localStorage?.setItem(STORAGE_KEY, JSON.stringify(settings));
  } catch {
    // Silently ignore quota / security restrictions.
  }
}

exports.STORAGE_KEY = STORAGE_KEY;
exports.getSettings = getSettings;
exports.updateSetting = updateSetting;
