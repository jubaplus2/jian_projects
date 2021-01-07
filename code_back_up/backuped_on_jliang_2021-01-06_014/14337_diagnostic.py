import warnings

from statsmodels.stats.diagnostic import (
    CompareCox, CompareJ, HetGoldfeldQuandt, OLS, ResultsStore, acorr_breusch_godfrey,
    acorr_ljungbox, acorr_lm, breaks_cusumolsresid, breaks_hansen, compare_cox, compare_j,
    het_arch, het_breuschpagan, het_goldfeldquandt, het_white, linear_harvey_collier, linear_lm,
    linear_rainbow, recursive_olsresiduals, spec_white, unitroot_adf
)
from statsmodels.tsa.stattools import adfuller

__all__ = ["CompareCox", "CompareJ", "HetGoldfeldQuandt", "OLS",
           "ResultsStore", "acorr_breusch_godfrey", "acorr_ljungbox",
           "acorr_lm", "adfuller", "breaks_cusumolsresid", "breaks_hansen",
           "compare_cox", "compare_j", "het_arch", "het_breuschpagan",
           "het_goldfeldquandt", "het_white", "linear_harvey_collier",
           "linear_lm", "linear_rainbow", "recursive_olsresiduals",
           "spec_white", "unitroot_adf"]


warnings.warn("The statsmodels.sandbox.stats.diagnostic module is deprecated. "
              "Use statsmodels.stats.diagnostic.", DeprecationWarning,
              stacklevel=2)