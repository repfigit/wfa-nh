# NH Immigrant Contracts Tracker

A web application for tracking New Hampshire state contracts with immigrant-related service providers, including refugee resettlement agencies, language access services, and social service organizations.

## Features

- **Contract Database**: Track state contracts with immigrant-related organizations
- **Fraud Indicators**: Automated flagging system for patterns that warrant review
- **Expenditure Tracking**: Import and analyze state expenditure data
- **Search & Filtering**: Find contracts by keyword, contractor, department, or amount
- **Data Sources**: Links to official NH government transparency portals

## Data Sources

This application aggregates data from official New Hampshire state government sources:

| Source | URL | Description |
|--------|-----|-------------|
| TransparentNH Expenditures | https://business.nh.gov/ExpenditureTransparency/ | State expenditure register |
| NH Child Care Licensing | https://www.dhhs.nh.gov/programs-services/childcare-parenting-childbirth/child-care-licensing | Child care provider licensing data |
| NH Connections (NHCIS) | https://www.nh-connections.org/ | Child care information system |
| Governor and Council Agendas | https://media.sos.nh.gov/govcouncil/ | Contract approval documents |
| CCDF State Plan 2025-2027 | https://www.dhhs.nh.gov/sites/g/files/ehbemt476/files/documents2/ccdf-state-plan-2025-2027.pdf | Child Care and Development Fund plan |
| Child Care Scholarship Program | https://www.dhhs.nh.gov/programs-services/childcare-parenting-childbirth/child-development-and-head-start/child-care | Scholarship program details |
| DHHS Contracts | https://www.dhhs.nh.gov/doing-business-dhhs/contracts-procurement-opportunities | Department-specific contracts |

**Note on Data Collection**: Live web scraping from government websites is currently blocked by anti-bot measures (403 Forbidden errors). The application uses sample/seed data for development and testing. Production data collection requires alternative access methods or API partnerships with state agencies.

## Known Contractors

The following immigrant-related contractors have been identified:

| Vendor Code | Name | Services |
|-------------|------|----------|
| 222201 | Ascentria Community Services, Inc. | Refugee resettlement, case management, language services |
| 177551 | International Institute of New England, Inc. | Refugee resettlement, ESL, employment services |
| - | Lutheran Immigration and Refugee Service | National resettlement agency |
| - | Catholic Charities NH | Immigration legal services |

## Fraud Indicators

The system automatically flags contracts with the following patterns:

- **Sole Source**: Contracts awarded without competitive bidding
- **Rapid Amendments**: Multiple contract amendments
- **Large Increases**: Significant value increases from original amount
- **Duplicate Payments**: Same amount paid to same vendor in short period
- **No Competition**: Contractors with many sole source awards

**Note**: These flags indicate areas for potential review, not accusations of wrongdoing.

## Installation

```bash
# Install dependencies
npm install

# Seed the database with known contracts
npm run seed

# Start the development server
npm run dev
```

## Usage

### Web Interface

Visit http://localhost:3000 to access the dashboard with:

- Overview statistics
- Recent contracts list
- Top contractors
- Fraud indicator summary

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| GET /api/dashboard | Dashboard statistics |
| GET /api/contracts | Search/list contracts |
| GET /api/contracts/:id | Contract details |
| GET /api/contractors | List contractors |
| GET /api/contractors/:id | Contractor details |
| GET /api/fraud-indicators | List fraud indicators |
| GET /api/expenditures | List expenditures |
| GET /api/reports/summary | Summary reports |

### Data Collection

**Current Limitations**: Automated web scraping from government websites is blocked by anti-bot protections. The application currently uses seed/sample data for development.

```bash
# Database seeding (sample data only)
npm run seed

# Scraping scripts (currently non-functional due to 403 blocks)
npm run scrape          # All scrapers (blocked)
npm run scrape:gc       # Governor & Council agendas (blocked)  
npm run scrape:expenditure # Expenditure data (blocked)
```

For production data collection, alternative methods are needed:
- Direct API access to state systems
- Manual data entry from public records
- Partnership with state agencies for data feeds

## Project Structure

```
├── src/
│   ├── api/           # Express API server
│   ├── db/            # Database schema and helpers
│   ├── scrapers/      # Data collection utilities
│   ├── analysis/      # Fraud indicator analysis
│   └── types/         # TypeScript type definitions
├── public/            # Frontend web interface
├── data/              # SQLite database files
└── package.json
```

## Contract Data Summary

Based on research of Governor & Council agendas (2023-2025):

### Ascentria Community Services
- Refugee Support Services: $769,637 (2 amendments, sole source)
- Ukrainian Resettlement: $251,910 (sole source)
- Case Management for Seniors: Sole source amendment
- Language Access Services: Competitive (amendment)
- ORR Support FY2025: $1,217,727 (74% increase)

### International Institute of New England
- Refugee Social Services: Sole source amendment
- School Services for Refugees: $844,000 (from $805,000)
- Expanded ORR Services: Sole source amendment

## Legal Disclaimer

This tool is for educational and research purposes. All data is sourced from publicly available government records. Fraud indicators are automated flags based on procurement patterns and do not constitute evidence of wrongdoing. Always verify information with official sources before drawing conclusions.

## License

MIT
