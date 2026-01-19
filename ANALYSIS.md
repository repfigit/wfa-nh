# NH Childcare Payments Tracker - Project Analysis

## Overview

**Purpose**: Fraud detection tool for tracking New Hampshire state childcare/daycare payments  
**Live URL**: https://wfa-nh.vercel.app  
**Tech Stack**: Express.js, Turso (SQLite), Trigger.dev, Vercel

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend                                 │
│                    public/index.html (2,400 LOC)                │
│                    Vanilla JS + CSS                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Vercel API Layer                           │
│                      api/index.ts (1,050 LOC)                   │
│                      Express + Serverless                       │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Turso DB      │  │  Trigger.dev    │  │   Scrapers      │
│   (SQLite)      │  │  Background     │  │   - TransparentNH│
│   11 tables     │  │  6 tasks        │  │   - USAspending │
│                 │  │                 │  │   - ACF CCDF    │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

---

## Findings

### 1. Security Issues

| Severity | Issue | Location | Status |
|----------|-------|----------|--------|
| HIGH | No authentication | All `/api/*` endpoints | **Needs Fix** |
| HIGH | Admin endpoints exposed | `/api/admin/*` | **Needs Fix** |
| MEDIUM | No input validation | All POST endpoints | **Needs Fix** |
| MEDIUM | SQL query params not typed | `api/index.ts:199` | **Needs Fix** |
| LOW | CORS allows all origins | `api/index.ts:43` | Consider restricting |

### 2. Code Quality Issues

| Issue | Location | Impact |
|-------|----------|--------|
| Monolithic HTML file | `public/index.html` (2,400 LOC) | Hard to maintain |
| No test coverage | - | Risk of regressions |
| Duplicate API code | `api/index.ts` + `src/api/server.ts` | DRY violation |
| Magic strings | Throughout codebase | Typo-prone |
| No TypeScript strict mode | `tsconfig.json` | Missing type safety |

### 3. API Design Issues

| Issue | Endpoints Affected | Fix |
|-------|-------------------|-----|
| No pagination | `/api/providers`, `/api/payments`, `/api/expenditures` | Add limit/offset |
| Inconsistent response format | Various | Standardize envelope |
| No rate limiting | All endpoints | Add rate limiter |
| No request validation | POST endpoints | Add zod/joi |

### 4. Database Issues

| Issue | Location | Impact |
|-------|----------|--------|
| No indexes defined | Schema | Slow queries at scale |
| N+1 query patterns | `dbHelpers` functions | Performance |
| No connection pooling | Turso client | Resource waste |

### 5. What's Working Well

- Clean separation of scrapers
- Good use of Trigger.dev for background jobs
- Decent error handling in async routes
- Environment-based DB switching (local/Turso)
- Comprehensive scraper coverage (5 data sources)

---

## Recommended Improvements

### Priority 1: Security (This Session)

1. **Add API Key Authentication**
   - Protect `/api/admin/*` endpoints
   - Add `x-api-key` header check
   - Store key in environment variable

2. **Add Input Validation**
   - Validate/sanitize all user inputs
   - Use parameterized queries (already doing this, but add types)

### Priority 2: Testing (This Session)

1. **Set up Vitest**
   - Add test framework
   - Write tests for fraud-detector
   - Write tests for API endpoints

### Priority 3: API Improvements (This Session)

1. **Add Pagination**
   - Standard limit/offset params
   - Return total count in response

### Priority 4: Future Improvements

1. Split `index.html` into components (React/Vue/Svelte)
2. Add database indexes
3. Implement rate limiting
4. Add request logging/monitoring
5. Set up CI/CD with tests

---

## Implementation Plan

```
[x] Fix TransparentNH scraper error handling
[ ] Add API key authentication for admin routes
[ ] Add input validation middleware
[ ] Set up Vitest testing framework
[ ] Add pagination to list endpoints
[ ] Deploy and test
```

---

## File Structure

```
wfa-nh/
├── api/
│   └── index.ts          # Vercel serverless API (1,050 LOC)
├── src/
│   ├── analyzer/
│   │   └── fraud-detector.ts    # Fraud detection algorithms
│   ├── db/
│   │   ├── database.ts          # Database helpers
│   │   ├── db-adapter.ts        # Turso/SQLite adapter
│   │   ├── schema.ts            # Table definitions
│   │   └── seed.ts              # Sample data
│   ├── importer/
│   │   └── csv-importer.ts      # CSV import logic
│   ├── scraper/
│   │   ├── transparent-nh-scraper.ts  # NH.gov (blocked)
│   │   ├── usaspending-scraper.ts     # Federal awards (working)
│   │   ├── acf-ccdf-scraper.ts        # HHS data (working)
│   │   ├── nh-das-scraper.ts          # State contracts
│   │   └── nh-licensing-scraper.ts    # Provider licensing
│   └── trigger/
│       └── *.ts                 # 6 Trigger.dev tasks
├── public/
│   └── index.html        # Frontend (2,400 LOC)
├── trigger.config.ts     # Trigger.dev config
└── package.json
```
