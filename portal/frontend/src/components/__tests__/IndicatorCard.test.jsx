/**
 * IndicatorCard Component Tests
 *
 * These tests require a testing library setup. To run:
 * 1. Install: npm install -D vitest @testing-library/react @testing-library/jest-dom jsdom
 * 2. Configure vitest.config.js with jsdom environment
 * 3. Run: npm test
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import IndicatorCard from '../IndicatorCard';

// Mock indicator data
const mockIndicator = {
  id: 'test-indicator-1',
  name: 'Test Moving Average',
  type: 'moving_average',
  params: {
    period: 20,
    lookback_window: 50,
    threshold: 0.5,
  },
  typed_outputs: [
    { name: 'breakout', type: 'signal', enabled: true },
    { name: 'retest', type: 'signal', enabled: false },
  ],
  enabled: true,
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString(),
};

const defaultProps = {
  indicator: mockIndicator,
  color: '#60a5fa',
  onToggle: vi.fn(),
  onEdit: vi.fn(),
  onDelete: vi.fn(),
  onDuplicate: vi.fn(),
  onGenerateSignals: vi.fn(),
  onSelectColor: vi.fn(),
  onRecompute: vi.fn(),
  isGeneratingSignals: false,
  disableSignalAction: false,
  selected: false,
  onSelectionToggle: vi.fn(),
  duplicatePending: false,
  busy: false,
  activeJobId: null,
};

describe('IndicatorCard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Information Architecture', () => {
    it('displays indicator name as primary text', () => {
      render(<IndicatorCard {...defaultProps} />);
      expect(screen.getByText('Test Moving Average')).toBeInTheDocument();
    });

    it('displays indicator type badge', () => {
      render(<IndicatorCard {...defaultProps} />);
      expect(screen.getByText('Moving Average')).toBeInTheDocument();
    });

    it('displays param summary pills in collapsed view', () => {
      render(<IndicatorCard {...defaultProps} />);
      // Params are formatted as Title Case
      expect(screen.getByText('Period')).toBeInTheDocument();
    });

    it('does NOT display "Ready" status badge for stable indicators', () => {
      render(<IndicatorCard {...defaultProps} />);
      expect(screen.queryByText('Ready')).not.toBeInTheDocument();
    });

    it('shows enabled signal summary in collapsed view', () => {
      render(<IndicatorCard {...defaultProps} />);
      expect(screen.getByText('breakout')).toBeInTheDocument();
      expect(screen.queryByText('retest')).not.toBeInTheDocument();
    });

    it('does NOT display "Awaiting first compute" text', () => {
      const indicatorWithoutUpdated = { ...mockIndicator, updated_at: null, created_at: null };
      render(<IndicatorCard {...defaultProps} indicator={indicatorWithoutUpdated} />);
      expect(screen.queryByText(/awaiting/i)).not.toBeInTheDocument();
    });
  });

  describe('Param Expand/Collapse', () => {
    it('starts in collapsed state', () => {
      render(<IndicatorCard {...defaultProps} />);
      // Expanded view shows raw snake_case keys
      expect(screen.queryByText('lookback_window')).not.toBeInTheDocument();
    });

    it('expands params when name is clicked', () => {
      render(<IndicatorCard {...defaultProps} />);
      const nameButton = screen.getByText('Test Moving Average');
      fireEvent.click(nameButton);
      // Expanded view shows all params with raw keys
      expect(screen.getByText('period')).toBeInTheDocument();
      expect(screen.getByText('lookback_window')).toBeInTheDocument();
    });

    it('collapses params when Collapse button is clicked', () => {
      render(<IndicatorCard {...defaultProps} />);
      // Expand first
      fireEvent.click(screen.getByText('Test Moving Average'));
      expect(screen.getByText('lookback_window')).toBeInTheDocument();

      // Collapse
      fireEvent.click(screen.getByText('Collapse'));
      expect(screen.queryByText('lookback_window')).not.toBeInTheDocument();
    });
  });

  describe('Visibility Toggle', () => {
    it('shows eye icon when indicator is visible', () => {
      render(<IndicatorCard {...defaultProps} indicator={{ ...mockIndicator, enabled: true }} />);
      expect(screen.getByLabelText('Hide overlay from chart')).toBeInTheDocument();
    });

    it('shows eye-off icon when indicator is hidden', () => {
      render(<IndicatorCard {...defaultProps} indicator={{ ...mockIndicator, enabled: false }} />);
      expect(screen.getByLabelText('Show overlay on chart')).toBeInTheDocument();
    });

    it('calls onToggle when visibility is toggled', () => {
      render(<IndicatorCard {...defaultProps} />);
      const visibilityButton = screen.getByLabelText('Hide overlay from chart');
      fireEvent.click(visibilityButton);
      expect(defaultProps.onToggle).toHaveBeenCalledWith('test-indicator-1');
    });

    it('applies desaturation styling when hidden', () => {
      const { container } = render(
        <IndicatorCard {...defaultProps} indicator={{ ...mockIndicator, enabled: false }} />
      );
      const card = container.firstChild;
      expect(card.className).toContain('opacity-60');
    });
  });

  describe('Blocking Compute UX', () => {
    it('hides Generate Signals when the indicator has no signal outputs', () => {
      render(
        <IndicatorCard
          {...defaultProps}
          indicator={{ ...mockIndicator, typed_outputs: [{ name: 'value_area', type: 'metric' }] }}
          showSignalAction={false}
        />
      );
      expect(screen.queryByText('Generate')).not.toBeInTheDocument();
    });

    it('disables Generate Signals button during signal generation', () => {
      render(<IndicatorCard {...defaultProps} isGeneratingSignals={true} />);
      const generateButton = screen.getByRole('button', { name: /working/i });
      expect(generateButton).toBeDisabled();
    });

    it('shows "Working" text during signal generation', () => {
      render(<IndicatorCard {...defaultProps} isGeneratingSignals={true} />);
      expect(screen.getByText('Working')).toBeInTheDocument();
    });

    it('disables Edit/Duplicate/Delete during busy state', () => {
      render(<IndicatorCard {...defaultProps} busy={true} />);
      // Open context menu
      fireEvent.click(screen.getByTitle('More actions'));

      // Delete should be disabled
      const deleteButton = screen.getByText('Delete');
      expect(deleteButton.closest('button')).toBeDisabled();
    });

    it('shows a direct edit action', () => {
      render(<IndicatorCard {...defaultProps} />);
      expect(screen.getByTitle('Edit indicator')).toBeInTheDocument();
    });

    it('keeps visibility toggle enabled during compute', () => {
      render(<IndicatorCard {...defaultProps} busy={true} activeJobId="test-indicator-1" />);
      const visibilityButton = screen.getByLabelText('Hide overlay from chart');
      // Visibility toggle should work during compute
      expect(visibilityButton).not.toBeDisabled();
    });
  });

  describe('Context Menu', () => {
    it('shows a compact overflow action list', () => {
      render(<IndicatorCard {...defaultProps} />);
      fireEvent.click(screen.getByTitle('More actions'));
      expect(screen.getByText('Recompute')).toBeInTheDocument();
      expect(screen.getByText('Duplicate')).toBeInTheDocument();
      expect(screen.getByText('Copy')).toBeInTheDocument();
      expect(screen.getByText('Delete')).toBeInTheDocument();
    });

    it('does not repeat Edit in the overflow menu', () => {
      render(<IndicatorCard {...defaultProps} />);
      fireEvent.click(screen.getByTitle('More actions'));
      expect(screen.queryByText('Open Editor')).not.toBeInTheDocument();
    });

    it('does not show section headers in the overflow menu', () => {
      render(<IndicatorCard {...defaultProps} />);
      fireEvent.click(screen.getByTitle('More actions'));
      expect(screen.queryByText('Runtime')).not.toBeInTheDocument();
      expect(screen.queryByText('Configuration')).not.toBeInTheDocument();
      expect(screen.queryByText('Danger')).not.toBeInTheDocument();
    });

    it('calls onDelete (opens modal) when Delete is clicked', () => {
      render(<IndicatorCard {...defaultProps} />);
      fireEvent.click(screen.getByTitle('More actions'));
      fireEvent.click(screen.getByText('Delete'));
      expect(defaultProps.onDelete).toHaveBeenCalledWith('test-indicator-1');
    });

    it('calls onRecompute when Recompute is clicked', () => {
      render(<IndicatorCard {...defaultProps} />);
      fireEvent.click(screen.getByTitle('More actions'));
      fireEvent.click(screen.getByText('Recompute'));
      expect(defaultProps.onRecompute).toHaveBeenCalledWith('test-indicator-1');
    });
  });

  describe('Transient Status Display', () => {
    it('shows status badge during creating state', () => {
      render(
        <IndicatorCard
          {...defaultProps}
          indicator={{ ...mockIndicator, _status: 'creating' }}
        />
      );
      expect(screen.getByText('Creating')).toBeInTheDocument();
    });

    it('shows status badge during computing state', () => {
      render(
        <IndicatorCard
          {...defaultProps}
          indicator={{ ...mockIndicator, _status: 'computing' }}
        />
      );
      expect(screen.getByText('Computing')).toBeInTheDocument();
    });

    it('shows error banner for failed state', () => {
      render(
        <IndicatorCard
          {...defaultProps}
          indicator={{ ...mockIndicator, _status: 'failed', _local: true }}
          onRetryCreate={vi.fn()}
          onRemoveLocal={vi.fn()}
        />
      );
      expect(screen.getByText(/indicator job failed/i)).toBeInTheDocument();
      expect(screen.getByText('Retry')).toBeInTheDocument();
    });
  });
});
