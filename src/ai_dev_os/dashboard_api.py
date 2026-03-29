from __future__ import annotations

from fastapi import FastAPI
from fastapi import Query
from fastapi.middleware.cors import CORSMiddleware

from ai_dev_os.dashboard_views import build_experiment_detail_view
from ai_dev_os.dashboard_views import build_experiment_list_view
from ai_dev_os.dashboard_views import build_flow_view
from ai_dev_os.dashboard_views import build_overview_view
from ai_dev_os.dashboard_views import build_trace_detail_view

app = FastAPI(title='AI Trading Dashboard API', version='v0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        'http://127.0.0.1:3000',
        'http://localhost:3000',
        'http://127.0.0.1:3001',
        'http://localhost:3001',
    ],
    allow_credentials=False,
    allow_methods=['GET'],
    allow_headers=['*'],
)


@app.get('/api/v1/overview')
def get_overview() -> dict:
    return build_overview_view()


@app.get('/api/v1/experiments')
def get_experiments(
    status: str = Query(default=''),
    strategy_family: str = Query(default=''),
    baseline_only: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    return build_experiment_list_view(
        status=status,
        strategy_family=strategy_family,
        baseline_only=baseline_only,
        limit=limit,
    )


@app.get('/api/v1/experiments/{experiment_id}')
def get_experiment_detail(experiment_id: str) -> dict:
    return build_experiment_detail_view(experiment_id)


@app.get('/api/v1/flow')
def get_flow() -> dict:
    return build_flow_view()


@app.get('/api/v1/traces/{run_id}')
def get_trace_detail(run_id: str) -> dict:
    return build_trace_detail_view(run_id)
