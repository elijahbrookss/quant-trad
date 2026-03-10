/**
 * DeleteIndicatorModal Component Tests
 *
 * These tests require a testing library setup. To run:
 * 1. Install: npm install -D vitest @testing-library/react @testing-library/jest-dom jsdom
 * 2. Configure vitest.config.js with jsdom environment
 * 3. Run: npm test
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import DeleteIndicatorModal from '../DeleteIndicatorModal';

// Mock the adapter
vi.mock('../../adapters/indicator.adapter', () => ({
  fetchIndicatorStrategies: vi.fn(),
}));

import { fetchIndicatorStrategies } from '../../adapters/indicator.adapter';

const defaultProps = {
  open: true,
  indicatorId: 'test-indicator-1',
  indicatorName: 'Test Moving Average',
  onClose: vi.fn(),
  onConfirm: vi.fn(),
};

describe('DeleteIndicatorModal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    fetchIndicatorStrategies.mockResolvedValue([]);
  });

  describe('Basic Rendering', () => {
    it('renders modal when open is true', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);
      expect(screen.getByText('Delete Indicator')).toBeInTheDocument();
    });

    it('shows indicator name in confirmation text', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);
      expect(screen.getByText(/Test Moving Average/)).toBeInTheDocument();
    });

    it('has a typed confirmation input', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);
      expect(screen.getByPlaceholderText('DELETE')).toBeInTheDocument();
    });
  });

  describe('Dependency Fetch', () => {
    it('shows loading state while fetching dependencies', async () => {
      fetchIndicatorStrategies.mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve([]), 1000))
      );
      render(<DeleteIndicatorModal {...defaultProps} />);
      expect(screen.getByText('Checking for dependencies...')).toBeInTheDocument();
    });

    it('displays strategies list when available', async () => {
      fetchIndicatorStrategies.mockResolvedValue([
        { id: 'strat-1', name: 'Strategy Alpha' },
        { id: 'strat-2', name: 'Strategy Beta' },
      ]);
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText('Strategy Alpha')).toBeInTheDocument();
        expect(screen.getByText('Strategy Beta')).toBeInTheDocument();
      });
    });

    it('shows "No dependent strategies" when none found', async () => {
      fetchIndicatorStrategies.mockResolvedValue([]);
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText('No strategies are using this indicator.')).toBeInTheDocument();
      });
    });

    it('handles fetch error gracefully', async () => {
      fetchIndicatorStrategies.mockRejectedValue(new Error('Network error'));
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText('Could not load impact data')).toBeInTheDocument();
        expect(screen.getByText('Network error')).toBeInTheDocument();
      });
    });

    it('shows caution message when fetch fails', async () => {
      fetchIndicatorStrategies.mockRejectedValue(new Error('Network error'));
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText(/Proceed with caution/)).toBeInTheDocument();
      });
    });
  });

  describe('Typed Confirmation', () => {
    it('confirm button is disabled until "DELETE" is typed', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        const confirmButton = screen.getByRole('button', { name: 'Delete Indicator' });
        expect(confirmButton).toBeDisabled();
      });
    });

    it('confirm button is enabled when "DELETE" is typed exactly', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        const input = screen.getByPlaceholderText('DELETE');
        fireEvent.change(input, { target: { value: 'DELETE' } });

        const confirmButton = screen.getByRole('button', { name: 'Delete Indicator' });
        expect(confirmButton).not.toBeDisabled();
      });
    });

    it('confirm button stays disabled for partial match', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        const input = screen.getByPlaceholderText('DELETE');
        fireEvent.change(input, { target: { value: 'DEL' } });

        const confirmButton = screen.getByRole('button', { name: 'Delete Indicator' });
        expect(confirmButton).toBeDisabled();
      });
    });

    it('converts input to uppercase automatically', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        const input = screen.getByPlaceholderText('DELETE');
        fireEvent.change(input, { target: { value: 'delete' } });

        expect(input.value).toBe('DELETE');
      });
    });
  });

  describe('Confirmation Flow', () => {
    it('calls onConfirm with indicator ID when confirmed', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        const input = screen.getByPlaceholderText('DELETE');
        fireEvent.change(input, { target: { value: 'DELETE' } });

        const confirmButton = screen.getByRole('button', { name: 'Delete Indicator' });
        fireEvent.click(confirmButton);
      });

      expect(defaultProps.onConfirm).toHaveBeenCalledWith('test-indicator-1');
    });

    it('shows loading state during deletion', async () => {
      defaultProps.onConfirm.mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, 1000))
      );
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(async () => {
        const input = screen.getByPlaceholderText('DELETE');
        fireEvent.change(input, { target: { value: 'DELETE' } });

        const confirmButton = screen.getByRole('button', { name: 'Delete Indicator' });
        fireEvent.click(confirmButton);

        expect(screen.getByText('Deleting...')).toBeInTheDocument();
      });
    });

    it('calls onClose when Cancel is clicked', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
      });

      expect(defaultProps.onClose).toHaveBeenCalled();
    });
  });

  describe('Modal State Reset', () => {
    it('resets confirmation text when modal closes and reopens', async () => {
      const { rerender } = render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        const input = screen.getByPlaceholderText('DELETE');
        fireEvent.change(input, { target: { value: 'DELETE' } });
        expect(input.value).toBe('DELETE');
      });

      // Close modal
      rerender(<DeleteIndicatorModal {...defaultProps} open={false} />);

      // Reopen modal
      rerender(<DeleteIndicatorModal {...defaultProps} open={true} />);

      await waitFor(() => {
        const input = screen.getByPlaceholderText('DELETE');
        expect(input.value).toBe('');
      });
    });
  });

  describe('Data Impact Warning', () => {
    it('shows warning about data that will be deleted', async () => {
      render(<DeleteIndicatorModal {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText(/Computed overlays, generated signals/)).toBeInTheDocument();
      });
    });
  });
});
