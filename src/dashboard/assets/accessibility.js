(function () {
  "use strict";

  function labelSlider(containerId, label) {
    const handle = document.querySelector(`#${containerId} [role="slider"]`);
    if (!handle) return;
    handle.setAttribute("aria-label", label);
    const value = handle.getAttribute("aria-valuenow");
    if (value !== null) {
      handle.setAttribute(
        "aria-valuetext",
        containerId === "threshold-slider" ? `${(Number(value) * 100).toFixed(1)} percent` : `${Number(value)} reviews per 1,000`
      );
    }
  }

  function applyLabels() {
    labelSlider("threshold-slider", "Minimum fraud probability");
    const queueRegion = document.querySelector("#queue-table .dash-spreadsheet-container");
    if (queueRegion) {
      queueRegion.setAttribute("tabindex", "0");
      queueRegion.setAttribute("role", "region");
      queueRegion.setAttribute("aria-label", "Bounded review queue");
    }
    const queueRows = document.querySelector("#queue-table .dt-table-container__row-1");
    if (queueRows) {
      queueRows.setAttribute("tabindex", "0");
      queueRows.setAttribute("role", "region");
      queueRows.setAttribute("aria-label", "Scrollable bounded review queue rows");
    }
    const targetRegion = document.querySelector("#target-table .dash-spreadsheet-container");
    if (targetRegion) {
      targetRegion.setAttribute("tabindex", "0");
      targetRegion.setAttribute("role", "region");
      targetRegion.setAttribute("aria-label", "Recall target workload scenarios");
    }
  }

  const observer = new MutationObserver(applyLabels);
  observer.observe(document.documentElement, { childList: true, subtree: true, attributes: true, attributeFilter: ["aria-valuenow"] });
  document.addEventListener("DOMContentLoaded", applyLabels);
  applyLabels();
})();
