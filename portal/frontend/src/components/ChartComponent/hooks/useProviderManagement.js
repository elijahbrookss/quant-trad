import { useState, useEffect, useCallback, useMemo } from 'react';
import { fetchProviders, saveProviderCredentials } from '../../../adapters/provider.adapter.js';

/**
 * useProviderManagement - Manages provider/venue selection and credentials
 *
 * Extracts provider-related state and logic from ChartComponent.
 * Part of ChartComponent refactoring to reduce complexity.
 */

// Helper functions (copied from ChartComponent)
const venueSlug = (venue) => {
  if (!venue) return null;
  const slug = venue.adapter_id || venue.id || venue.value;
  return typeof slug === 'string' && slug.trim() ? slug.trim().toLowerCase() : null;
};

const venueOptionsForProvider = (providers, providerId) => {
  const normalized = (providerId || '').toString().trim().toUpperCase();
  const provider = (providers || []).find((item) => item.id === normalized);
  return (provider?.venues || []).map((venue) => ({
    value: venue.id,
    label: venue.label,
    slug: venueSlug(venue),
  }));
};

const providerToDatasource = (providerId) => {
  const normalized = (providerId || '').toString().trim().toUpperCase();
  if (normalized === 'YAHOO') return 'YFINANCE';
  if (normalized === 'INTERACTIVE_BROKERS') return 'IBKR';
  if (normalized) return normalized;
  return 'YFINANCE'; // DEFAULT_DATASOURCE
};

export function useProviderManagement({
  savedPrefs = {},
  logger,
  onDatasourceChange,
  onExchangeChange,
}) {
  // Provider state
  const [providers, setProviders] = useState([]);
  const [providersLoading, setProvidersLoading] = useState(false);
  const [providerId, setProviderId] = useState(() => savedPrefs?.providerId || '');
  const [venueId, setVenueId] = useState(() => savedPrefs?.venueId || '');

  // Credentials state
  const [credentialsModal, setCredentialsModal] = useState({
    open: false,
    providerId: null,
    venueId: null,
    required: [],
  });
  const [credentialsInputs, setCredentialsInputs] = useState({});
  const [credentialsSaving, setCredentialsSaving] = useState(false);
  const [credentialsError, setCredentialsError] = useState(null);
  const [credentialsSavedAt, setCredentialsSavedAt] = useState(0);

  // Reload providers from API
  const reloadProviders = useCallback(async () => {
    setProvidersLoading(true);
    try {
      const response = await fetchProviders();
      logger?.debug?.('providers_response', response);
      const items = response?.providers || [];
      setProviders(items);
      if (!providerId && items.length > 0) {
        const fallback = items[0];
        setProviderId(fallback.id);
        const firstVenue = (fallback.venues || [])[0];
        if (firstVenue) {
          setVenueId(firstVenue.id);
          if (typeof onExchangeChange === 'function') {
            onExchangeChange(venueSlug(firstVenue) || '');
          }
        }
        if (typeof onDatasourceChange === 'function') {
          onDatasourceChange(providerToDatasource(fallback.id));
        }
      }
    } catch (err) {
      logger?.warn?.('provider_fetch_failed', err);
    } finally {
      setProvidersLoading(false);
    }
  }, [logger, providerId, onDatasourceChange, onExchangeChange]);

  // Load providers on mount
  useEffect(() => {
    let mounted = true;
    void (async () => {
      await reloadProviders();
      if (!mounted) return;
    })();
    return () => {
      mounted = false;
    };
  }, [reloadProviders]);

  // Update datasource when providerId changes
  useEffect(() => {
    const mapped = providerToDatasource(providerId);
    if (typeof onDatasourceChange === 'function') {
      onDatasourceChange(mapped);
    }
  }, [providerId, onDatasourceChange]);

  // Sync venue when provider or venues change
  useEffect(() => {
    const venueOpts = venueOptionsForProvider(providers, providerId);
    if (!venueOpts.length) return;
    const hasMatch = venueOpts.some((item) => item.value === venueId);
    const nextVenue = hasMatch ? venueId : venueOpts[0].value;
    if (nextVenue !== venueId) {
      setVenueId(nextVenue);
    }
    const slug = venueOpts.find((item) => item.value === nextVenue)?.slug || null;
    if (slug && typeof onExchangeChange === 'function') {
      onExchangeChange(slug);
    }
  }, [providers, providerId, venueId, onExchangeChange]);

  // Provider status map
  const providerStatusMap = useMemo(() => {
    const map = {};
    (providers || []).forEach((item) => {
      map[item.id] = item.status || { state: 'available', missing: [], required: [] };
    });
    return map;
  }, [providers]);

  // Provider options for dropdown
  const providerOptions = useMemo(
    () =>
      (providers || []).map((item) => ({
        value: item.id,
        label: item.label,
      })),
    [providers]
  );

  // Venue options for current provider
  const venueOptions = useMemo(() => {
    const items = venueOptionsForProvider(providers, providerId);
    return items.map((item) => {
      const venue = (providers || [])
        .flatMap((p) => p.venues || [])
        .find((v) => v.id === item.value);
      return {
        ...item,
      };
    });
  }, [providers, providerId]);

  // Selected venue value
  const selectedVenueValue = useMemo(
    () => venueId || venueOptions[0]?.value || '',
    [venueId, venueOptions]
  );

  // Provider and venue status
  const selectedProviderStatus =
    providerStatusMap[providerId] || { state: 'available', missing: [], required: [] };
  const selectedVenueStatus = useMemo(() => {
    const venue = (providers || [])
      .flatMap((p) => p.venues || [])
      .find((v) => v.id === selectedVenueValue);
    return venue?.status || { state: 'available', missing: [], required: [] };
  }, [providers, selectedVenueValue]);

  const providerBlocked = false;

  // Log provider selection changes
  useEffect(() => {
    logger?.debug?.('provider_selection_changed', {
      providerId,
      venueId: selectedVenueValue,
      providerStatus: selectedProviderStatus,
      venueStatus: selectedVenueStatus,
    });
  }, [logger, providerId, selectedVenueValue, selectedProviderStatus, selectedVenueStatus]);

  // Handle provider change
  const handleProviderChange = useCallback(
    (nextId) => {
      const normalized = (nextId || '').toString().trim().toUpperCase();
      setProviderId(normalized);
      const venues = venueOptionsForProvider(providers, normalized);
      const firstVenue = venues[0];
      if (firstVenue) {
        setVenueId(firstVenue.value);
        if (typeof onExchangeChange === 'function') {
          onExchangeChange(firstVenue.slug || '');
        }
      } else {
        setVenueId('');
        if (typeof onExchangeChange === 'function') {
          onExchangeChange('');
        }
      }
    },
    [providers, onExchangeChange]
  );

  // Handle venue change
  const handleVenueChange = useCallback(
    (nextVenue) => {
      const normalized = (nextVenue || '').toString().trim().toUpperCase();
      setVenueId(normalized);
      const venue = venueOptions.find((item) => item.value === normalized);
      if (typeof onExchangeChange === 'function') {
        onExchangeChange(venue?.slug || '');
      }
    },
    [venueOptions, onExchangeChange]
  );

  // Open credentials modal
  const openCredentialsModal = useCallback((provider, venue, required = []) => {
    const req = Array.isArray(required) ? required : [];
    setCredentialsInputs(req.reduce((acc, key) => ({ ...acc, [key]: '' }), {}));
    setCredentialsError(null);
    setCredentialsModal({ open: true, providerId: provider, venueId: venue, required: req });
  }, []);

  // Close credentials modal
  const closeCredentialsModal = useCallback(() => {
    setCredentialsModal({ open: false, providerId: null, venueId: null, required: [] });
    setCredentialsInputs({});
    setCredentialsError(null);
  }, []);

  // Save credentials
  const handleSaveCredentials = useCallback(async () => {
    if (!credentialsModal.providerId || credentialsModal.required.length === 0) {
      closeCredentialsModal();
      return;
    }
    setCredentialsSaving(true);
    setCredentialsError(null);
    try {
      await saveProviderCredentials({
        provider_id: credentialsModal.providerId,
        venue_id: credentialsModal.venueId,
        credentials: credentialsInputs,
      });
      await reloadProviders();
      setCredentialsSavedAt(Date.now());
      closeCredentialsModal();
    } catch (err) {
      setCredentialsError(err?.message || 'Unable to save credentials.');
    } finally {
      setCredentialsSaving(false);
    }
  }, [credentialsModal, credentialsInputs, reloadProviders, closeCredentialsModal]);

  return {
    // State
    providers,
    providersLoading,
    providerId,
    setProviderId,
    venueId,
    setVenueId,

    // Computed values
    providerStatusMap,
    providerOptions,
    venueOptions,
    selectedVenueValue,
    selectedProviderStatus,
    selectedVenueStatus,
    providerBlocked,

    // Handlers
    handleProviderChange,
    handleVenueChange,
    reloadProviders,

    // Credentials
    credentialsModal,
    credentialsInputs,
    setCredentialsInputs,
    credentialsSaving,
    credentialsError,
    credentialsSavedAt,
    openCredentialsModal,
    closeCredentialsModal,
    handleSaveCredentials,
  };
}
