from __future__ import annotations

from typing import Any
from typing import Callable

import optuna

from ai_dev_os.project_objects import VariantSearchSpec
from ai_dev_os.project_objects import validate_variant_search_spec


TrialResult = dict[str, Any]


def _suggest_value(trial: optuna.Trial, name: str, spec: dict[str, Any]) -> Any:
    param_type = str(spec.get("type", "") or "")
    if param_type == "int":
        return trial.suggest_int(name, int(spec["low"]), int(spec["high"]), step=int(spec.get("step", 1)))
    if param_type == "float":
        return trial.suggest_float(name, float(spec["low"]), float(spec["high"]), step=spec.get("step"))
    if param_type == "categorical":
        return trial.suggest_categorical(name, list(spec.get("choices", [])))
    raise ValueError(f"unsupported parameter type for {name}: {param_type}")


def run_variant_search(
    variant_search_spec: VariantSearchSpec | dict[str, Any],
    *,
    objective_fn: Callable[[dict[str, Any]], float],
) -> dict[str, Any]:
    spec = validate_variant_search_spec(variant_search_spec)
    study = optuna.create_study(direction=spec["objective_mode"])

    def _objective(trial: optuna.Trial) -> float:
        params = {name: _suggest_value(trial, name, param_spec) for name, param_spec in spec["parameter_space"].items()}
        return float(objective_fn(params))

    study.optimize(_objective, n_trials=spec["max_trials"])
    trials: list[TrialResult] = []
    for trial in study.trials:
        if trial.state != optuna.trial.TrialState.COMPLETE:
            continue
        trials.append(
            {
                "trial_number": int(trial.number),
                "params": dict(trial.params),
                "value": float(trial.value),
                "state": trial.state.name.lower(),
            }
        )
    reverse = spec["objective_mode"] == "maximize"
    trials.sort(key=lambda item: item["value"], reverse=reverse)
    return {
        "search_id": spec["search_id"],
        "objective_metric": spec["objective_metric"],
        "objective_mode": spec["objective_mode"],
        "best_params": dict(study.best_params),
        "best_value": float(study.best_value),
        "trials": trials,
    }
