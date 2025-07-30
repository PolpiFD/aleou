# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a hotel information extraction application that scrapes data from Cvent, Google Maps, and hotel websites using Playwright, Firecrawl, and OpenAI APIs. The application provides a Streamlit web interface for batch processing hotels from CSV files or individual URLs.

## Essential Commands

### Development
```bash
# Run the application
streamlit run main.py

# Run tests
pytest tests/ -v --tb=short

# Run tests with coverage
pytest --cov=modules --cov=cache --cov=config --cov=utils --cov-report=term-missing

# Install dependencies
pip install -r requirements.txt
playwright install chromium
```

### Deployment
```bash
# Deploy to production (requires VPS access)
./deploy.sh production

# GitHub Actions automatically deploys on push to main branch
```

## Architecture Overview

### Core Components

1. **Streamlit Interface** (`main.py`, `ui/`): Web interface for CSV/URL extraction
   - CSV mode: Batch process multiple hotels
   - URL mode: Single hotel extraction
   - Real-time progress tracking and error handling

2. **Extraction Modules** (`modules/`):
   - `cvent_extractor.py`: Playwright-based Cvent scraping (Grid/Popup interfaces)
   - `firecrawl_extractor.py`: Firecrawl API integration for website data
   - `gmaps_extractor.py`: Google Maps API integration
   - `website_finder.py`: Find official hotel websites
   - `parallel_processor.py`: Concurrent processing for large batches

3. **Cvent Scraping** (`salles_cvent/`): Legacy extraction logic
   - `detect_button.py`: Interface type detection
   - `extract_data_grid.py`: Grid interface extraction
   - `extract_data_popup.py`: Popup interface extraction

4. **Configuration** (`config/settings.py`):
   - Environment variables loaded from `.env`
   - API keys: OPENAI_API_KEY, GOOGLE_MAPS_API_KEY, FIRECRAWL_API_KEY
   - Rate limiting and parallel processing settings

## Key Implementation Details

- **Rate Limiting**: Implemented for all APIs (Google Maps: 10/s, OpenAI: 5/s)
- **Caching**: Google Maps results cached to reduce API calls
- **Error Recovery**: Automatic retries with exponential backoff
- **CSV Output**: Consolidated files with one row per meeting room
- **Playwright**: Headless mode by default, 25s timeout per page

## Environment Variables

Required in `.env`:
- `OPENAI_API_KEY`: For website data extraction
- `GOOGLE_MAPS_API_KEY`: For location data
- `FIRECRAWL_API_KEY`: Optional, falls back to legacy mode if missing

## Testing Approach

The project uses pytest with coverage reporting. Tests are located in `tests/` and cover processors and cache functionality. Run tests before committing any changes to ensure nothing is broken.