/**
 * Entity Resolution Module
 * Matches provider records from various sources to the authoritative provider_master table
 * using composite scoring across multiple fields.
 */

import { query, execute } from '../db/db-adapter.js';

// =====================================================
// Types
// =====================================================

export interface MatchCandidate {
  providerId: number;
  ccisId: string | null;
  name: string;
  normalizedName: string;
  address: string | null;
  city: string | null;
  zip: string | null;
  score: number;
  matchDetails: MatchDetail[];
}

export interface MatchDetail {
  criterion: string;
  sourceValue: string;
  matchedValue: string;
  score: number;
  weight: number;
}

export interface MatchConfig {
  nameWeight: number;
  addressWeight: number;
  cityWeight: number;
  zipWeight: number;
  phoneWeight: number;
  autoMatchThreshold: number;
  reviewThreshold: number;
  rejectThreshold: number;
}

export interface ResolveInput {
  name: string;
  address?: string | null;
  city?: string | null;
  zip?: string | null;
  phone?: string | null;
  sourceSystem: string;
  sourceIdentifier?: string;
}

export interface ResolveResult {
  matched: boolean;
  providerId: number | null;
  ccisId: string | null;
  score: number;
  matchMethod: string;
  matchDetails: MatchDetail[];
  needsReview: boolean;
}

// =====================================================
// Default Configuration
// =====================================================

export const DEFAULT_MATCH_CONFIG: MatchConfig = {
  nameWeight: 0.35,
  addressWeight: 0.30,
  cityWeight: 0.15,
  zipWeight: 0.15,
  phoneWeight: 0.05,
  autoMatchThreshold: 0.85,
  reviewThreshold: 0.60,
  rejectThreshold: 0.40,
};

// =====================================================
// String Normalization Functions
// =====================================================

/**
 * Normalize a provider/business name for comparison
 */
export function normalizeName(name: string): string {
  if (!name) return '';

  return name
    .toUpperCase()
    .replace(/[^\w\s]/g, ' ')  // Remove punctuation
    .replace(/\b(INC|LLC|CORP|CORPORATION|CO|COMPANY|THE|OF|AND|A|AN)\b/g, '')
    .replace(/\b(CHILDCARE|CHILD CARE|DAYCARE|DAY CARE)\b/g, 'CHILDCARE')
    .replace(/\b(CENTER|CENTRE|CTR)\b/g, 'CENTER')
    .replace(/\b(PRESCHOOL|PRE-SCHOOL|PRE SCHOOL)\b/g, 'PRESCHOOL')
    .replace(/\b(LEARNING|LEARNG)\b/g, 'LEARNING')
    .replace(/\b(EARLY|ERALY)\b/g, 'EARLY')
    .replace(/\b(FAMILY|FAM)\b/g, 'FAMILY')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Normalize a street address for comparison
 */
export function normalizeAddress(address: string): string {
  if (!address) return '';

  return address
    .toUpperCase()
    .replace(/\b(STREET|STR)\b/g, 'ST')
    .replace(/\b(AVENUE|AVE)\b/g, 'AVE')
    .replace(/\b(ROAD|RD)\b/g, 'RD')
    .replace(/\b(DRIVE|DR)\b/g, 'DR')
    .replace(/\b(LANE|LN)\b/g, 'LN')
    .replace(/\b(BOULEVARD|BLVD)\b/g, 'BLVD')
    .replace(/\b(COURT|CT)\b/g, 'CT')
    .replace(/\b(CIRCLE|CIR)\b/g, 'CIR')
    .replace(/\b(PLACE|PL)\b/g, 'PL')
    .replace(/\b(NORTH|N)\b/g, 'N')
    .replace(/\b(SOUTH|S)\b/g, 'S')
    .replace(/\b(EAST|E)\b/g, 'E')
    .replace(/\b(WEST|W)\b/g, 'W')
    .replace(/\b(APARTMENT|APT)\b/g, 'APT')
    .replace(/\b(SUITE|STE)\b/g, 'STE')
    .replace(/\b(UNIT|UN)\b/g, 'UNIT')
    .replace(/[^\w\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Normalize phone number to 10 digits
 */
export function normalizePhone(phone: string | null | undefined): string {
  if (!phone) return '';

  const digits = phone.replace(/\D/g, '');

  // Handle 11-digit numbers starting with 1
  if (digits.length === 11 && digits.startsWith('1')) {
    return digits.substring(1);
  }

  return digits.length === 10 ? digits : '';
}

/**
 * Extract first 5 digits of ZIP code
 */
export function normalizeZip(zip: string | null | undefined): string {
  if (!zip) return '';
  return zip.replace(/\D/g, '').substring(0, 5);
}

// =====================================================
// String Similarity Functions
// =====================================================

/**
 * Jaro similarity algorithm
 */
function jaroSimilarity(s1: string, s2: string): number {
  if (s1 === s2) return 1.0;
  if (s1.length === 0 || s2.length === 0) return 0.0;

  const matchDistance = Math.floor(Math.max(s1.length, s2.length) / 2) - 1;
  const s1Matches = new Array(s1.length).fill(false);
  const s2Matches = new Array(s2.length).fill(false);

  let matches = 0;
  let transpositions = 0;

  // Find matches
  for (let i = 0; i < s1.length; i++) {
    const start = Math.max(0, i - matchDistance);
    const end = Math.min(i + matchDistance + 1, s2.length);

    for (let j = start; j < end; j++) {
      if (s2Matches[j] || s1[i] !== s2[j]) continue;
      s1Matches[i] = true;
      s2Matches[j] = true;
      matches++;
      break;
    }
  }

  if (matches === 0) return 0.0;

  // Count transpositions
  let k = 0;
  for (let i = 0; i < s1.length; i++) {
    if (!s1Matches[i]) continue;
    while (!s2Matches[k]) k++;
    if (s1[i] !== s2[k]) transpositions++;
    k++;
  }

  return (
    (matches / s1.length +
      matches / s2.length +
      (matches - transpositions / 2) / matches) /
    3
  );
}

/**
 * Jaro-Winkler similarity algorithm (gives bonus for common prefix)
 */
export function jaroWinkler(s1: string, s2: string, prefixScale = 0.1): number {
  const jaro = jaroSimilarity(s1, s2);

  // Calculate common prefix (up to 4 characters)
  let prefix = 0;
  const maxPrefix = Math.min(4, Math.min(s1.length, s2.length));
  for (let i = 0; i < maxPrefix; i++) {
    if (s1[i] === s2[i]) {
      prefix++;
    } else {
      break;
    }
  }

  return jaro + prefix * prefixScale * (1 - jaro);
}

/**
 * Simple containment check - useful for partial matches
 */
export function containmentScore(s1: string, s2: string): number {
  if (!s1 || !s2) return 0;

  const shorter = s1.length <= s2.length ? s1 : s2;
  const longer = s1.length > s2.length ? s1 : s2;

  if (longer.includes(shorter)) {
    return shorter.length / longer.length;
  }

  return 0;
}

// =====================================================
// Entity Resolution Functions
// =====================================================

/**
 * Find the best matching provider from provider_master
 */
export async function findBestMatch(
  input: ResolveInput,
  config: MatchConfig = DEFAULT_MATCH_CONFIG
): Promise<MatchCandidate | null> {
  const normalizedInputName = normalizeName(input.name);
  const normalizedAddress = input.address ? normalizeAddress(input.address) : null;
  const normalizedZip = normalizeZip(input.zip);
  const normalizedPhone = normalizePhone(input.phone);
  const normalizedCity = input.city?.toUpperCase().trim() || null;

  if (!normalizedInputName) {
    return null;
  }

  // Get candidates from provider_master + aliases
  // Use prefix matching to narrow down candidates
  const namePrefix = normalizedInputName.substring(0, 10);

  const candidates = await query(`
    SELECT DISTINCT
      pm.id,
      pm.ccis_provider_id,
      pm.canonical_name,
      pm.name_display,
      pm.address_normalized,
      pm.city,
      pm.zip5,
      pm.phone_normalized
    FROM provider_master pm
    LEFT JOIN provider_aliases pa ON pm.id = pa.provider_master_id
    WHERE pm.is_active = 1
      AND (
        pm.canonical_name LIKE ?
        OR pa.alias_normalized LIKE ?
        OR (pm.city = ? AND pm.zip5 = ?)
      )
    LIMIT 100
  `, [
    `%${namePrefix}%`,
    `%${namePrefix}%`,
    normalizedCity || '',
    normalizedZip || '',
  ]);

  if (candidates.length === 0) {
    return null;
  }

  // Score each candidate
  const scored: MatchCandidate[] = [];

  for (const candidate of candidates) {
    const details: MatchDetail[] = [];
    let totalScore = 0;
    let totalWeight = 0;

    // Name score (Jaro-Winkler)
    const candidateName = candidate.canonical_name as string;
    const nameScore = jaroWinkler(normalizedInputName, candidateName);

    // Also check containment for partial matches
    const containment = containmentScore(normalizedInputName, candidateName);
    const bestNameScore = Math.max(nameScore, containment * 0.9);

    details.push({
      criterion: 'name',
      sourceValue: normalizedInputName,
      matchedValue: candidateName,
      score: bestNameScore,
      weight: config.nameWeight,
    });
    totalScore += bestNameScore * config.nameWeight;
    totalWeight += config.nameWeight;

    // Address score
    if (normalizedAddress && candidate.address_normalized) {
      const addrScore = jaroWinkler(normalizedAddress, candidate.address_normalized as string);
      details.push({
        criterion: 'address',
        sourceValue: normalizedAddress,
        matchedValue: candidate.address_normalized as string,
        score: addrScore,
        weight: config.addressWeight,
      });
      totalScore += addrScore * config.addressWeight;
      totalWeight += config.addressWeight;
    }

    // City score (exact match)
    if (normalizedCity && candidate.city) {
      const cityScore = normalizedCity === (candidate.city as string).toUpperCase() ? 1.0 : 0.0;
      details.push({
        criterion: 'city',
        sourceValue: normalizedCity,
        matchedValue: candidate.city as string,
        score: cityScore,
        weight: config.cityWeight,
      });
      totalScore += cityScore * config.cityWeight;
      totalWeight += config.cityWeight;
    }

    // ZIP score
    if (normalizedZip && candidate.zip5) {
      const zipScore = normalizedZip === (candidate.zip5 as string) ? 1.0 : 0.0;
      details.push({
        criterion: 'zip',
        sourceValue: normalizedZip,
        matchedValue: candidate.zip5 as string,
        score: zipScore,
        weight: config.zipWeight,
      });
      totalScore += zipScore * config.zipWeight;
      totalWeight += config.zipWeight;
    }

    // Phone score
    if (normalizedPhone && candidate.phone_normalized) {
      const phoneScore = normalizedPhone === (candidate.phone_normalized as string) ? 1.0 : 0.0;
      details.push({
        criterion: 'phone',
        sourceValue: normalizedPhone,
        matchedValue: candidate.phone_normalized as string,
        score: phoneScore,
        weight: config.phoneWeight,
      });
      totalScore += phoneScore * config.phoneWeight;
      totalWeight += config.phoneWeight;
    }

    // Normalize score by actual weights used
    const finalScore = totalWeight > 0 ? totalScore / totalWeight * (totalWeight / 1.0) : 0;

    scored.push({
      providerId: candidate.id as number,
      ccisId: candidate.ccis_provider_id as string | null,
      name: candidate.name_display as string,
      normalizedName: candidateName,
      address: candidate.address_normalized as string | null,
      city: candidate.city as string | null,
      zip: candidate.zip5 as string | null,
      score: finalScore,
      matchDetails: details,
    });
  }

  // Sort by score descending and return the best
  scored.sort((a, b) => b.score - a.score);

  const best = scored[0];
  if (best && best.score >= config.rejectThreshold) {
    return best;
  }

  return null;
}

/**
 * Resolve an entity from a source system to provider_master
 * Handles auto-matching, review queueing, and audit logging
 */
export async function resolveEntity(
  input: ResolveInput,
  config: MatchConfig = DEFAULT_MATCH_CONFIG
): Promise<ResolveResult> {
  const result: ResolveResult = {
    matched: false,
    providerId: null,
    ccisId: null,
    score: 0,
    matchMethod: 'none',
    matchDetails: [],
    needsReview: false,
  };

  // First check if this source identifier already has a link
  if (input.sourceIdentifier) {
    const existingLink = await query(`
      SELECT provider_master_id, match_score, match_method
      FROM provider_source_links
      WHERE source_system = ? AND source_identifier = ? AND status = 'active'
      LIMIT 1
    `, [input.sourceSystem, input.sourceIdentifier]);

    if (existingLink.length > 0) {
      const link = existingLink[0];
      result.matched = true;
      result.providerId = link.provider_master_id as number;
      result.score = link.match_score as number || 1.0;
      result.matchMethod = 'existing_link';
      return result;
    }
  }

  // Find best match
  const match = await findBestMatch(input, config);

  if (!match) {
    result.matchMethod = 'no_candidates';
    return result;
  }

  result.score = match.score;
  result.matchDetails = match.matchDetails;

  if (match.score >= config.autoMatchThreshold) {
    // High confidence - auto-link
    result.matched = true;
    result.providerId = match.providerId;
    result.ccisId = match.ccisId;
    result.matchMethod = 'auto_match';

    // Create source link if we have an identifier
    if (input.sourceIdentifier) {
      await createSourceLink(
        match.providerId,
        input.sourceSystem,
        input.sourceIdentifier,
        input.name,
        'auto_match',
        match.score,
        match.matchDetails
      );
    }

    // Log the match
    await logMatchAudit(
      match.providerId,
      input.sourceSystem,
      input.sourceIdentifier || input.name,
      input.name,
      'matched',
      match.score,
      'auto_match',
      match.matchDetails
    );

  } else if (match.score >= config.reviewThreshold) {
    // Medium confidence - queue for review
    result.matchMethod = 'needs_review';
    result.needsReview = true;
    result.providerId = match.providerId; // Tentative match

    // Add to pending matches queue
    if (input.sourceIdentifier) {
      await createPendingMatch(
        input.sourceSystem,
        input.sourceIdentifier,
        input.name,
        input.address || null,
        input.city || null,
        input.zip || null,
        match.providerId,
        match.score,
        match.matchDetails
      );
    }
  } else {
    // Below review threshold
    result.matchMethod = 'low_confidence';
  }

  return result;
}

// =====================================================
// Database Helper Functions
// =====================================================

/**
 * Create a source link between a provider_master record and an external source
 */
export async function createSourceLink(
  providerMasterId: number,
  sourceSystem: string,
  sourceIdentifier: string,
  sourceName: string,
  matchMethod: string,
  matchScore: number,
  matchDetails: MatchDetail[]
): Promise<void> {
  try {
    await execute(`
      INSERT OR REPLACE INTO provider_source_links (
        provider_master_id, source_system, source_identifier, source_name,
        match_method, match_score, match_details, status, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', datetime('now'))
    `, [
      providerMasterId,
      sourceSystem,
      sourceIdentifier,
      sourceName,
      matchMethod,
      matchScore,
      JSON.stringify(matchDetails),
    ]);
  } catch (err) {
    console.error('Error creating source link:', err);
  }
}

/**
 * Log a match decision to the audit log
 */
export async function logMatchAudit(
  providerMasterId: number | null,
  sourceSystem: string,
  sourceIdentifier: string,
  sourceName: string,
  action: string,
  matchScore?: number,
  matchMethod?: string,
  matchDetails?: MatchDetail[]
): Promise<void> {
  try {
    await execute(`
      INSERT INTO match_audit_log (
        provider_master_id, source_system, source_identifier, source_name,
        action, match_score, match_method, match_details
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      providerMasterId,
      sourceSystem,
      sourceIdentifier,
      sourceName,
      action,
      matchScore || null,
      matchMethod || null,
      matchDetails ? JSON.stringify(matchDetails) : null,
    ]);
  } catch (err) {
    console.error('Error logging match audit:', err);
  }
}

/**
 * Add a record to the pending matches queue for manual review
 */
export async function createPendingMatch(
  sourceSystem: string,
  sourceIdentifier: string,
  sourceName: string,
  sourceAddress: string | null,
  sourceCity: string | null,
  sourceZip: string | null,
  candidateProviderId: number,
  matchScore: number,
  matchDetails: MatchDetail[]
): Promise<void> {
  try {
    await execute(`
      INSERT OR IGNORE INTO pending_matches (
        source_system, source_identifier, source_name,
        source_address, source_city, source_zip,
        candidate_provider_id, match_score, match_details, status
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
    `, [
      sourceSystem,
      sourceIdentifier,
      sourceName,
      sourceAddress,
      sourceCity,
      sourceZip,
      candidateProviderId,
      matchScore,
      JSON.stringify(matchDetails),
    ]);
  } catch (err) {
    console.error('Error creating pending match:', err);
  }
}

/**
 * Add an alias to a provider
 */
export async function addProviderAlias(
  providerMasterId: number,
  aliasName: string,
  aliasType: string,
  source: string,
  confidence: number = 1.0
): Promise<void> {
  const normalizedAlias = normalizeName(aliasName);

  if (!normalizedAlias) return;

  try {
    await execute(`
      INSERT OR IGNORE INTO provider_aliases (
        provider_master_id, alias_name, alias_normalized, alias_type, source, confidence
      ) VALUES (?, ?, ?, ?, ?, ?)
    `, [
      providerMasterId,
      aliasName,
      normalizedAlias,
      aliasType,
      source,
      confidence,
    ]);
  } catch (err) {
    console.error('Error adding provider alias:', err);
  }
}

/**
 * Get provider_master_id from an existing source link
 */
export async function getProviderMasterIdFromSource(
  sourceSystem: string,
  sourceIdentifier: string
): Promise<number | null> {
  const result = await query(`
    SELECT provider_master_id FROM provider_source_links
    WHERE source_system = ? AND source_identifier = ? AND status = 'active'
    LIMIT 1
  `, [sourceSystem, sourceIdentifier]);

  return result.length > 0 ? (result[0].provider_master_id as number) : null;
}

export default {
  normalizeName,
  normalizeAddress,
  normalizePhone,
  normalizeZip,
  jaroWinkler,
  findBestMatch,
  resolveEntity,
  createSourceLink,
  logMatchAudit,
  createPendingMatch,
  addProviderAlias,
  getProviderMasterIdFromSource,
  DEFAULT_MATCH_CONFIG,
};
