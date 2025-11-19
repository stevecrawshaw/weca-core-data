# ETL Refactoring Strategies for WECA Core Data

## Executive Summary

This document outlines three ranked strategies for refactoring the WECA Core Data ETL codebase to follow modern software design principles. Each strategy builds on the previous one, allowing for incremental implementation.

> **📘 See Also:** For a critical evaluation of using the **dlt (data load tool)** framework as an alternative to these custom strategies, refer to **[DLT_EVALUATION.md](./DLT_EVALUATION.md)**. The evaluation includes a recommended **hybrid approach** that combines dlt for REST API extraction with custom code for transformations.

**Current State:**
- 1,044 lines in `get_ca_data.py` with 29 functions (procedural style)
- 542 lines in `cesap-epc-load-duckdb-data.py` (Jupyter-style cells)
- 66+ DuckDB operations scattered throughout orchestration script
- No test coverage, no OOP structure, synchronous HTTP calls only
- Mixed use of `requests` (synchronous) despite `httpx` being in dependencies

---

## Strategy 1: Modularize into Layered Architecture (HIGH PRIORITY)

**Effort:** Medium | **Impact:** High | **Risk:** Low

### Rationale

The current codebase violates the Single Responsibility Principle with all 29 functions in one 1,000+ line module. ETL best practices demand separation of concerns across Extract, Transform, and Load layers.

### Implementation Plan

#### 1.1 Create Layer-Based Module Structure

```
weca_core_data/
├── __init__.py
├── extractors/
│   ├── __init__.py
│   ├── base.py              # Abstract base extractor class
│   ├── arcgis.py            # ArcGIS REST API extraction
│   ├── epc.py               # EPC API extraction
│   ├── ons.py               # ONS data extraction
│   └── nomis.py             # NOMIS API extraction
├── transformers/
│   ├── __init__.py
│   ├── base.py              # Abstract transformer
│   ├── geography.py         # LSOA, postcode transforms
│   ├── epc.py               # EPC data cleaning/validation
│   └── schema.py            # Move epc_schema.py here
├── loaders/
│   ├── __init__.py
│   ├── base.py              # Abstract loader
│   ├── duckdb_loader.py     # DuckDB-specific operations
│   └── sql_templates.py     # Move build_tables_queries.py here
├── config/
│   ├── __init__.py
│   ├── settings.py          # Pydantic settings model
│   └── urls.yaml            # Externalize url_dict
├── models/
│   ├── __init__.py
│   └── schemas.py           # Pydantic models for data validation
└── utils/
    ├── __init__.py
    ├── logging.py           # Centralized logging config
    ├── validation.py        # URL validation, etc.
    └── file_ops.py          # Directory creation, file deletion
```

#### 1.2 Design Pattern: Template Method for ETL Operations

```python
# extractors/base.py
from abc import ABC, abstractmethod
from pathlib import Path
import polars as pl

class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.logger = self._setup_logger()

    @abstractmethod
    def extract(self) -> pl.DataFrame:
        """Extract data from source."""
        pass

    @abstractmethod
    def validate_source(self) -> bool:
        """Validate data source is accessible."""
        pass

    def save_raw(self, df: pl.DataFrame, filename: str) -> Path:
        """Save raw extracted data to parquet."""
        path = self.output_dir / filename
        df.write_parquet(path)
        self.logger.info(f"Saved raw data to {path}")
        return path
```

#### 1.3 Migration Path

**Phase 1:** Create new structure alongside existing code
- Keep `get_ca_data.py` functional
- Create new modules with refactored functions
- Add comprehensive type hints and docstrings

**Phase 2:** Update `cesap-epc-load-duckdb-data.py` to use new modules
- Replace `import get_ca_data as get_ca` with layer imports
- Maintain backward compatibility during transition

**Phase 3:** Deprecate old modules
- Add deprecation warnings to old functions
- Remove old code once tests pass

### Benefits

- **Maintainability:** Each module has clear responsibility
- **Testability:** Smaller, focused modules are easier to test
- **Reusability:** Extract/Transform/Load components can be used independently
- **Discoverability:** New developers can navigate by ETL phase

### Metrics for Success

- Average module size < 250 lines
- Each function < 50 lines
- Clear import paths (`from weca_core_data.extractors.arcgis import ArcGISExtractor`)

---

## Strategy 2: Implement Configuration Management & Dependency Injection (MEDIUM PRIORITY)

**Effort:** Medium | **Impact:** High | **Risk:** Low

### Rationale

Currently, configuration is hardcoded in scripts (urls, paths, download flags). Modern ETL systems use environment-aware configuration with validation. This enables:
- Different configs for dev/staging/prod
- Secrets management (EPC credentials)
- Easy parameter tuning without code changes

### Implementation Plan

#### 2.1 Pydantic Settings Model

```python
# config/settings.py
from pathlib import Path
from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class WECASettings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="WECA_",
        case_sensitive=False
    )

    # Paths
    data_dir: Path = Field(default=Path("data"))
    epc_bulk_dir: Path = Field(default=Path("data/epc_bulk_zips"))
    db_path: Path = Field(default=Path("data/ca_epc.duckdb"))

    # Feature flags
    download_epc: bool = Field(default=False)
    download_lsoa: bool = Field(default=False)
    download_postcodes: bool = Field(default=False)

    # API credentials
    epc_api_key: SecretStr | None = Field(default=None)

    # URLs (loaded from YAML via custom validator)
    arcgis_base_url: HttpUrl = Field(...)
    epc_base_url: HttpUrl = Field(...)

    # Processing
    chunk_size: int = Field(default=10000)
    max_retries: int = Field(default=3)
    timeout_seconds: int = Field(default=10)

    # Logging
    log_level: str = Field(default="INFO")
    log_file: Path = Field(default=Path("etl.log"))

# Usage
settings = WECASettings()
```

#### 2.2 Externalize URLs to YAML

```yaml
# config/urls.yaml
arcgis:
  base_url: "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
  endpoints:
    lsoa_2021_pwc: "LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/"
    lsoa_2021_poly: "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/"
    lsoa_2011_poly: "LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/"

epc:
  base_url: "https://epc.opendatacommunities.org/api/v1/"
  endpoints:
    files: "files/"
    domestic: "domestic/search"

data_sources:
  ghg_emissions: "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv"
  imd: "https://opendatacommunities.org/downloads/cube-table?uri=http%3A%2F%2Fopendatacommunities.org%2Fdata%2Fsocietal-wellbeing%2Fimd2019%2Findices"
  nomis_ts054: "https://www.nomisweb.co.uk/api/v01/dataset/NM_2072_1.data.csv"
  dft_traffic: "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv"
```

#### 2.3 Dependency Injection Pattern

```python
# Example: Injecting config into extractors
class EPCExtractor(BaseExtractor):
    def __init__(
        self,
        settings: WECASettings,
        http_client: httpx.Client,
        output_dir: Path | None = None
    ):
        self.settings = settings
        self.client = http_client
        super().__init__(output_dir or settings.epc_bulk_dir)

    def extract(self) -> pl.DataFrame:
        headers = {"Authorization": self.settings.epc_api_key.get_secret_value()}
        # Use injected client instead of creating new requests
        response = self.client.get(
            str(self.settings.epc_base_url),
            headers=headers,
            timeout=self.settings.timeout_seconds
        )
        ...
```

### Benefits

- **Environment-aware:** Dev/staging/prod configs without code changes
- **Type-safe:** Pydantic validates all settings at startup
- **Testable:** Inject mock configs/clients for testing
- **Secure:** Secrets in `.env` files, not committed to git
- **Maintainable:** URLs in YAML, easy to update without code changes

### Migration Path

1. Create `config/settings.py` with Pydantic model
2. Create `.env.example` with all required variables
3. Update extractors to accept `settings` parameter
4. Update orchestration script to instantiate with settings
5. Move all hardcoded values to config

---

## Strategy 3: Add Async I/O, Retry Logic & Observability (LOWER PRIORITY)

**Effort:** High | **Impact:** High | **Risk:** Medium

### Rationale

Current implementation uses synchronous `requests` for all HTTP calls, despite having `httpx` and `asyncio` in dependencies. ETL workloads are I/O-bound; async operations can provide 3-10x throughput improvement when downloading from multiple sources.

Additionally, production ETL systems need:
- Automatic retry with exponential backoff
- Circuit breakers for failing APIs
- Detailed metrics and tracing

### Implementation Plan

#### 3.1 Async HTTP with httpx

```python
# extractors/arcgis.py
import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

class ArcGISExtractor(BaseExtractor):
    def __init__(self, settings: WECASettings):
        super().__init__(settings.data_dir)
        self.settings = settings
        self.client = httpx.AsyncClient(
            timeout=settings.timeout_seconds,
            limits=httpx.Limits(max_connections=10)
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def _fetch_feature_layer(
        self,
        service: str,
        params: dict[str, str | int]
    ) -> dict:
        """Fetch data from ArcGIS FeatureServer with retry logic."""
        url = f"{self.settings.arcgis_base_url}{service}/FeatureServer/0/query"

        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error fetching {url}: {e}")
            raise

    async def extract_all_lsoa_2021(self) -> pl.DataFrame:
        """Extract all LSOA 2021 boundaries in parallel chunks."""
        # Get total count first
        count_params = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
        count_data = await self._fetch_feature_layer("LSOA_2021", count_params)
        total = count_data["count"]

        # Create chunk tasks
        chunk_size = 2000
        tasks = []
        for offset in range(0, total, chunk_size):
            params = {
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": chunk_size
            }
            tasks.append(self._fetch_feature_layer("LSOA_2021", params))

        # Execute all chunks concurrently
        results = await asyncio.gather(*tasks)

        # Combine results into Polars DataFrame
        all_features = [f for r in results for f in r["features"]]
        return pl.DataFrame([f["attributes"] for f in all_features])
```

#### 3.2 Observability with Structured Logging & Metrics

```python
# utils/logging.py
import structlog
from pythonjsonlogger import jsonlogger

def setup_logging(settings: WECASettings) -> None:
    """Configure structured logging with JSON output."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
    )

# Usage in extractors
logger = structlog.get_logger()
logger.info(
    "extraction_started",
    extractor="ArcGISExtractor",
    dataset="lsoa_2021",
    total_records=total
)
```

#### 3.3 Add Metrics Collection

```python
# utils/metrics.py
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json

@dataclass
class ETLMetrics:
    """Metrics for ETL job execution."""
    job_name: str
    start_time: datetime
    end_time: datetime | None = None
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    errors: list[str] = None

    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    def save(self, path: Path) -> None:
        """Save metrics to JSON file."""
        with path.open("w") as f:
            json.dump(self.__dict__, f, default=str, indent=2)

# Usage in orchestration
metrics = ETLMetrics(job_name="epc_extraction", start_time=datetime.now())
try:
    df = await extractor.extract()
    metrics.records_extracted = len(df)
finally:
    metrics.end_time = datetime.now()
    metrics.save(Path("data/metrics/epc_extraction.json"))
```

#### 3.4 Circuit Breaker Pattern

```python
# utils/resilience.py
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=60)
async def resilient_api_call(
    client: httpx.AsyncClient,
    url: str,
    **kwargs
) -> httpx.Response:
    """API call with circuit breaker protection."""
    response = await client.get(url, **kwargs)
    response.raise_for_status()
    return response
```

### Benefits

- **Performance:** 3-10x faster data extraction through parallelization
- **Reliability:** Automatic retry handles transient failures
- **Observability:** Structured logs enable monitoring and debugging
- **Production-ready:** Circuit breakers prevent cascade failures
- **Metrics:** Track job performance and data quality over time

### Migration Path

1. Add dependencies: `uv add httpx tenacity structlog python-json-logger circuitbreaker`
2. Create async versions of extractors alongside sync versions
3. Update orchestration to use `asyncio.run()` for async extractors
4. Add structured logging incrementally
5. Implement metrics collection for critical jobs
6. Remove sync implementations once async versions are proven

### Performance Expectations

**Before:**
- Sequential download of 10 LSOA datasets: ~180 seconds
- Single-threaded EPC bulk downloads: ~45 minutes

**After:**
- Parallel download of 10 LSOA datasets: ~25 seconds (7x improvement)
- Concurrent EPC bulk downloads: ~8 minutes (5.6x improvement)

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Implement Strategy 1: Create modular architecture
- [ ] Migrate 5 most-used functions to new structure
- [ ] Add comprehensive type hints
- [ ] Set up pytest framework

### Phase 2: Configuration (Week 3)
- [ ] Implement Strategy 2: Pydantic settings
- [ ] Externalize URLs to YAML
- [ ] Create `.env.example` with all settings
- [ ] Update orchestration script to use config

### Phase 3: Testing & Documentation (Week 4)
- [ ] Write unit tests for extractors (target: 70% coverage)
- [ ] Write integration tests for full ETL pipeline
- [ ] Document all public APIs
- [ ] Create example notebooks using new architecture

### Phase 4: Performance (Weeks 5-6) - OPTIONAL
- [ ] Implement Strategy 3: Async extractors
- [ ] Add retry logic and circuit breakers
- [ ] Implement structured logging
- [ ] Add metrics collection
- [ ] Performance benchmarking

---

## Risk Mitigation

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Breaking existing notebooks/workflows | High | High | Maintain backward compatibility layer during migration |
| Async complexity introduces bugs | Medium | Medium | Keep sync versions until async proven in production |
| Configuration management overhead | Low | Medium | Use sensible defaults, .env optional for local dev |
| Performance regression | Low | High | Benchmark before/after, roll back if slower |

### Organizational Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Resistance to change | Medium | Medium | Incremental rollout, demonstrate benefits early |
| Knowledge transfer | Medium | High | Comprehensive documentation, pair programming |
| Timeline pressure | High | Medium | Strategies are independent; implement highest ROI first |

---

## Success Criteria

### Code Quality Metrics
- ✅ Ruff linting: 0 errors, 0 warnings
- ✅ mypy type coverage: > 90%
- ✅ Test coverage: > 70% (unit + integration)
- ✅ Average cyclomatic complexity: < 10

### Performance Metrics
- ✅ Full ETL pipeline runtime: < 15 minutes (currently ~60 minutes)
- ✅ Memory usage: < 8GB peak (profile with memory-profiler)
- ✅ DuckDB load time: < 2 minutes for all tables

### Maintainability Metrics
- ✅ Average module size: < 250 lines
- ✅ Average function size: < 50 lines
- ✅ Documentation: All public APIs have docstrings
- ✅ Onboarding time for new developer: < 1 day

---

## Alternative: dlt Framework

Before implementing these custom strategies, consider evaluating the **dlt (data load tool)** framework as documented in **[DLT_EVALUATION.md](./DLT_EVALUATION.md)**.

**Key Findings:**
- **dlt excels at:** REST API extraction with automatic pagination, schema inference, DuckDB integration
- **dlt struggles with:** Complex Polars transformations, custom file operations, spatial data processing
- **Recommended approach:** **Hybrid** - Use dlt for 70% (API extraction) + custom code for 30% (transformations)
- **Time comparison:**
  - Full custom (all 3 strategies): 6 weeks
  - Full dlt adoption: 2.5 weeks (but less flexibility)
  - **Hybrid approach: 4 weeks** (optimal balance)

The hybrid approach would replace much of Strategy 1 and Strategy 2 with dlt's built-in capabilities, while maintaining custom code for complex transformations. Strategy 3 (async) could still be selectively applied to performance-critical paths not handled by dlt.

---

## Conclusion

These three strategies provide a clear path from the current procedural codebase to a modern, maintainable ETL system:

1. **Strategy 1** (modularization) provides the foundation for all improvements and should be implemented first.
2. **Strategy 2** (configuration) enables environment-aware deployment and is essential for production use.
3. **Strategy 3** (async + observability) is optional but provides significant performance gains for large-scale operations.

Each strategy follows the Python code guidelines in `agent-docs/` and leverages modern Python 3.12+ features. The incremental approach minimizes risk while delivering value at each phase.

**However**, before starting, review **[DLT_EVALUATION.md](./DLT_EVALUATION.md)** to determine if a hybrid approach using the dlt framework would be more efficient for your specific use case.
