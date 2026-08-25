---
name: Financial Payments Fraud Decision Workbench
description: A calm operational risk desk for capacity-bound fraud review.
colors:
  canvas: "#f3f1eb"
  surface: "#fffdf8"
  ink: "#171915"
  ink-muted: "#5d625a"
  line: "#d8d7cf"
  control: "#255f73"
  control-deep: "#174553"
  review: "#c26b27"
  captured: "#19734b"
  missed: "#a53f3f"
typography:
  display:
    fontFamily: "Arial Narrow, Aptos Narrow, system-ui, sans-serif"
    fontSize: "clamp(2.25rem, 5vw, 4.75rem)"
    fontWeight: 700
    lineHeight: 0.96
    letterSpacing: "-0.03em"
  body:
    fontFamily: "Aptos, system-ui, -apple-system, Segoe UI, sans-serif"
    fontSize: "0.95rem"
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: "Aptos, system-ui, -apple-system, Segoe UI, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 700
    lineHeight: 1.2
rounded:
  control: "6px"
  surface: "14px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "16px"
  lg: "24px"
  xl: "32px"
components:
  button-primary:
    backgroundColor: "{colors.control}"
    textColor: "{colors.surface}"
    rounded: "{rounded.control}"
    padding: "10px 16px"
  panel:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.surface}"
    padding: "24px"
---

# Design System: Financial Payments Fraud Decision Workbench

## Overview

**Creative North Star: "The Review Ledger"**

The interface borrows the discipline of an operations ledger: dense enough for investigation, quiet enough for prolonged use, and explicit about every consequence. Its character comes from strong typographic hierarchy, ruled surfaces, and one muted blue control voice. It rejects generic equal-card dashboards and neon threat-map theater.

Key characteristics:

- A first viewport organized around the policy equation: threshold, capacity, and consequences.
- Tabular detail stays visually connected to aggregate outcomes.
- Status is written as text and reinforced with color, never encoded by color alone.

## Colors

Warm neutral surfaces reduce glare in a desk-lit operating scene. Muted blue owns interaction; amber, green, and red are reserved for review outcomes.

## Typography

Condensed system display type gives the workbench a ledger-like title without adding a network font dependency. Body and data use the platform UI stack. Labels are concise and sentence case. Numeric content uses tabular figures.

## Layout

The desktop shell uses a 12-column grid with a maximum width of 1480 px. The policy controls and consequences share the first viewport. Below 860 px, all regions become one column, tables scroll horizontally, and controls retain full labels. Spacing follows a 4 px base with 8, 16, 24, and 32 px steps.

## Elevation & Depth

The system is flat by default. Tonal layers and single-pixel rules establish hierarchy. A low offset shadow appears only on the sticky context bar and selected record drawer.

## Shapes

Controls use 6 px corners. Major surfaces use 14 px corners. Pills are limited to compact state labels. Borders remain one pixel.

## Components

Policy controls show their current value, valid range, and effect. Consequence metrics pair a number with a one-sentence definition. The queue uses a sticky header, visible row selection, deterministic rank, and native CSV export. Empty and error states name the cause and recovery.

The director brief leads with the active operating point. Detection coverage, review economics, and observed source amount form separate ruled groups. The capacity frontier precedes model diagnostics so workload decisions stay ahead of score mechanics.

## Do's and Don'ts

### Do:

- Do identify the binding control beside the policy controls.
- Do show undefined ratios as unavailable, with a reason.
- Do label feature contributions as model signals.

### Don't:

- Don't fabricate merchant, customer, or card context.
- Don't use causal language for anonymized PCA features.
- Don't hide missing or corrupt artifacts behind a zero-valued dashboard.
