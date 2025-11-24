import pandas as pd
import numpy as np

def calculate_capped_weights(
    weights: pd.Series,
    cap_limit: float = 0.10,
    max_iterations: int = 100,
    tolerance: float = 1e-12
) -> pd.Series:
    """
    Parameters
    ----------
    weights : pd.Series
        Initial (uncapped) weights, must sum to 1.0
    cap_limit : float
        Individual stock cap (e.g. 0.10 for 10%)
    max_iterations : int
        Safety limit
    tolerance : float
        Stop when no weight exceeds cap by more than this

    Returns
    -------
    pd.Series
        Final capped weights (sum to 1.0)
    """
    if abs(weights.sum() - 1.0) > 1e-10:
        raise ValueError("Input weights must sum to 1.0")

    current_weights = weights.copy()

    for iteration in range(1, max_iterations + 1):
        # Find stocks currently exceeding the cap (use rounding for stability)
        violators = current_weights.round(14) > cap_limit

        if not violators.any():
            break

        n_capped = violators.sum()
        target_weight_for_uncapped = 1.0 - n_capped * cap_limit
        current_weight_of_uncapped = current_weights[~violators].sum()

        if current_weight_of_uncapped <= tolerance:
            # Numerical edge case: everything is capped
            break

        # This is the KEY line — exact analytical proportional boost
        boost_ratio = target_weight_for_uncapped / current_weight_of_uncapped

        # Apply: capped → exactly cap_limit, uncapped → boosted proportionally
        current_weights = np.where(
            violators,
            cap_limit,
            current_weights * boost_ratio
        )

        # Renormalize exactly to 1.0 (eliminates floating-point drift)
        current_weights = current_weights / current_weights.sum()

    else:
        raise RuntimeError(f"Did not converge after {max_iterations} iterations")

    return pd.Series(current_weights, index=weights.index)