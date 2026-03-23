import os

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

try:
    from lightgbm import LGBMClassifier
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    from xgboost import XGBClassifier


class ModeloSeleccion:
    BASE_COLUMNS = [
        "nit_enmascarado",
        "num_oblig_orig_enmascarado",
        "num_oblig_enmascarado",
        "fecha",
    ]

    PIVOT_FEATURES = [
        "vlr_obligacion",
        "vlr_vencido",
        "saldo_capital",
        "endeudamiento",
        "cant_alter_posibles",
    ]

    CUSTOMER_FEATURES = [
        "edad_cli",
        "total_ing",
        "tot_activos",
        "tot_pasivos",
        "egresos_mes",
        "tot_patrimonio",
        "segm",
        "subsegm",
        "region_of",
        "tipo_cli",
        "ocup",
        "act_econom",
    ]

    SCORE_FEATURES = ["prob_propension", "prob_auto_cura", "prob_alrt_temprana"]
    PAY_FREQ_FEATURES = ["pct_meses_pagados_6m", "meses_desde_ultimo_pago"]
    PAY_HISTORY_FEATURES = [
        "hist_n_periodos",
        "hist_porc_pago_med",
        "hist_porc_pago_mean",
        "hist_porc_pago_std",
        "hist_prop_pago_comp",
        "hist_pago_total_med",
        "hist_pago_ultimos3",
    ]
    SCORE_HISTORY_FEATURES = [
        "score_prop_hist_mean",
        "score_prop_hist_std",
        "score_autocura_mean",
        "score_alrt_mean",
        "score_hist_n",
    ]

    PIVOT_NUMERIC_FEATURES = [
        "vlr_obligacion",
        "vlr_vencido",
        "saldo_capital",
        "endeudamiento",
        "cant_alter_posibles",
    ]

    CUSTOMER_NUMERIC_FEATURES = [
        "edad_cli",
        "total_ing",
        "tot_activos",
        "tot_pasivos",
        "egresos_mes",
        "tot_patrimonio",
    ]

    @staticmethod
    def _construir_id(df, id_col, id_columns):
        if id_col in df.columns:
            return df[id_col].astype(str)
        columnas_faltantes = [col for col in id_columns if col not in df.columns]
        if columnas_faltantes:
            raise ValueError(f"No se puede construir el ID. Faltan columnas: {columnas_faltantes}")
        return df[id_columns].astype(str).agg("#".join, axis=1)

    @staticmethod
    def _find_first_column(df, candidates, required=True):
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        if required:
            raise ValueError(f"No se encontró ninguna columna entre: {candidates}")
        return None

    @staticmethod
    def _parse_date_series(series):
        text = series.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        length = text.str.len()

        parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

        mask_yyyymmdd = length == 8
        if mask_yyyymmdd.any():
            parsed.loc[mask_yyyymmdd] = pd.to_datetime(
                text.loc[mask_yyyymmdd], format="%Y%m%d", errors="coerce"
            )

        mask_yyyymm = length == 6
        if mask_yyyymm.any():
            parsed.loc[mask_yyyymm] = pd.to_datetime(
                text.loc[mask_yyyymm] + "01", format="%Y%m%d", errors="coerce"
            )

        mask_other = parsed.isna()
        if mask_other.any():
            parsed.loc[mask_other] = pd.to_datetime(text.loc[mask_other], errors="coerce")

        return parsed

    @staticmethod
    def _coerce_numeric_columns(df, columns):
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _normalize_base(self, df, is_train):
        base = df.copy()
        rename_map = {}
        date_col = self._find_first_column(base, ["fecha", "fecha_var_rpta_alt", "fecha_corte"])
        if date_col != "fecha":
            rename_map[date_col] = "fecha"
        target_col = self._find_first_column(base, ["var_rpta_alt", "target"], required=False)
        if is_train and target_col and target_col != "var_rpta_alt":
            rename_map[target_col] = "var_rpta_alt"
        base = base.rename(columns=rename_map)
        base["fecha"] = self._parse_date_series(base["fecha"])
        base = self._coerce_numeric_columns(base, self.PIVOT_NUMERIC_FEATURES)
        if "var_rpta_alt" in base.columns:
            base["var_rpta_alt"] = pd.to_numeric(base["var_rpta_alt"], errors="coerce")
        return base

    def _normalize_payments(self, df):
        payments = df.copy()
        date_col = self._find_first_column(payments, ["fecha", "fecha_corte"])
        if date_col != "fecha":
            payments = payments.rename(columns={date_col: "fecha"})
        payments["fecha"] = self._parse_date_series(payments["fecha"])
        payments = self._coerce_numeric_columns(payments, ["pago_total", "valor_cuota_mes", "porc_pago"])
        if "porc_pago" not in payments.columns and {"pago_total", "valor_cuota_mes"}.issubset(payments.columns):
            payments["porc_pago"] = np.where(
                payments["valor_cuota_mes"].fillna(0) > 0,
                payments["pago_total"].fillna(0) / payments["valor_cuota_mes"].replace(0, np.nan),
                0,
            )
        return payments

    def _normalize_customers(self, df):
        customers = df.copy()
        date_col = self._find_first_column(customers, ["fecha", "fecha_corte"], required=False)
        if date_col is not None:
            if date_col != "fecha":
                customers = customers.rename(columns={date_col: "fecha"})
            customers["fecha"] = self._parse_date_series(customers["fecha"])
        elif {"year", "month"}.issubset(customers.columns):
            year = pd.to_numeric(customers["year"], errors="coerce")
            month = pd.to_numeric(customers["month"], errors="coerce")
            customers["fecha"] = pd.to_datetime(
                {
                    "year": year,
                    "month": month,
                    "day": 1,
                },
                errors="coerce",
            )
        else:
            raise ValueError("La tabla de customer no contiene fecha ni columnas year/month para derivarla")
        customers = self._coerce_numeric_columns(customers, self.CUSTOMER_NUMERIC_FEATURES)
        return customers.sort_values(["nit_enmascarado", "fecha"])

    def _normalize_scores(self, df):
        scores = df.copy()
        date_col = self._find_first_column(scores, ["fecha", "fecha_corte"])
        if date_col != "fecha":
            scores = scores.rename(columns={date_col: "fecha"})
        scores["fecha"] = self._parse_date_series(scores["fecha"])
        scores = self._coerce_numeric_columns(scores, self.SCORE_FEATURES)
        scores["score_month"] = scores["fecha"].dt.to_period("M")
        return scores

    def _build_pivot_features(self, base_df, is_train):
        df = base_df[[col for col in self.BASE_COLUMNS if col in base_df.columns]].copy()
        for col in self.PIVOT_FEATURES:
            df[col] = base_df[col] if col in base_df.columns else np.nan
        if is_train:
            df["target"] = base_df["var_rpta_alt"].astype(float)
        return df

    def _add_payment_recency_frequency_features(self, pivot_features, payments_df, window_months=6):
        pivot_keys = pivot_features[["nit_enmascarado", "num_oblig_enmascarado", "fecha"]].drop_duplicates().copy()
        pivot_keys["pivot_month"] = pivot_keys["fecha"].dt.to_period("M")

        payments = payments_df.copy()
        payments["year_month"] = payments["fecha"].dt.to_period("M")
        payments["flag_pago_mes"] = (payments["pago_total"].fillna(0) > 0).astype(int)
        payments = payments[["nit_enmascarado", "num_oblig_enmascarado", "year_month", "flag_pago_mes"]]

        payments_filtered = payments.merge(
            pivot_keys[["nit_enmascarado", "num_oblig_enmascarado"]].drop_duplicates(),
            on=["nit_enmascarado", "num_oblig_enmascarado"],
            how="inner",
        )

        last_payment = (
            payments_filtered.loc[payments_filtered["flag_pago_mes"] == 1]
            .groupby(["nit_enmascarado", "num_oblig_enmascarado"])["year_month"]
            .max()
            .reset_index()
            .rename(columns={"year_month": "ultimo_pago_mes"})
        )

        payments_with_pivot = payments_filtered.merge(
            pivot_keys,
            on=["nit_enmascarado", "num_oblig_enmascarado"],
            how="inner",
        )

        payments_window = payments_with_pivot.loc[
            (payments_with_pivot["year_month"] < payments_with_pivot["pivot_month"])
            & (payments_with_pivot["year_month"] >= payments_with_pivot["pivot_month"] - window_months)
        ]

        freq = (
            payments_window.groupby(["nit_enmascarado", "num_oblig_enmascarado", "pivot_month"])["flag_pago_mes"]
            .mean()
            .reset_index()
            .rename(columns={"flag_pago_mes": f"pct_meses_pagados_{window_months}m"})
        )

        recency = pivot_keys.merge(
            last_payment,
            on=["nit_enmascarado", "num_oblig_enmascarado"],
            how="left",
        )
        recency["meses_desde_ultimo_pago"] = (
            recency["pivot_month"] - recency["ultimo_pago_mes"]
        ).apply(lambda value: value.n if pd.notnull(value) else window_months + 1)

        payments_features = freq.merge(
            recency[["nit_enmascarado", "num_oblig_enmascarado", "pivot_month", "meses_desde_ultimo_pago"]],
            on=["nit_enmascarado", "num_oblig_enmascarado", "pivot_month"],
            how="outer",
        )
        payments_features["fecha"] = payments_features["pivot_month"].dt.to_timestamp()

        return pivot_features.merge(
            payments_features.drop(columns=["pivot_month"]),
            on=["nit_enmascarado", "num_oblig_enmascarado", "fecha"],
            how="left",
        )

    def _add_customer_snapshot(self, pivot_features, customers_df):
        customer_cols = [col for col in self.CUSTOMER_FEATURES if col in customers_df.columns]
        customer_df = customers_df[["nit_enmascarado", "fecha"] + customer_cols].copy()
        customer_df = customer_df.dropna(subset=["fecha", "nit_enmascarado"])
        customer_df = customer_df.sort_values(["fecha", "nit_enmascarado"]).reset_index(drop=True)

        left = pivot_features.copy()
        left["__row_order"] = np.arange(len(left))
        left_non_null = left.dropna(subset=["fecha", "nit_enmascarado"]).copy()
        left_non_null = left_non_null.sort_values(["fecha", "nit_enmascarado"]).reset_index(drop=True)

        merged = pd.merge_asof(
            left_non_null,
            customer_df,
            by="nit_enmascarado",
            on="fecha",
            direction="backward",
        )

        left_null = left[left["fecha"].isna()].copy()
        if not left_null.empty:
            for col in customer_cols:
                left_null[col] = np.nan
            merged = pd.concat([merged, left_null], ignore_index=True, sort=False)

        merged = merged.sort_values("__row_order").drop(columns=["__row_order"])
        return merged.reset_index(drop=True)

    def _add_score_lag(self, pivot_features, scores_df):
        score_cols = [col for col in self.SCORE_FEATURES if col in scores_df.columns]
        scores_prepared = scores_df[["nit_enmascarado", "num_oblig_enmascarado", "score_month"] + score_cols].copy()
        pivot_keys = pivot_features[["nit_enmascarado", "num_oblig_enmascarado", "fecha"]].copy()
        pivot_keys["pivot_month"] = pivot_keys["fecha"].dt.to_period("M")
        pivot_keys["score_month"] = pivot_keys["pivot_month"] - 1
        scores_features = pivot_keys.merge(
            scores_prepared,
            on=["nit_enmascarado", "num_oblig_enmascarado", "score_month"],
            how="left",
        ).drop(columns=["pivot_month", "score_month"], errors="ignore")

        return pivot_features.merge(
            scores_features,
            on=["nit_enmascarado", "num_oblig_enmascarado", "fecha"],
            how="left",
        )

    def _build_history_features(self, obs_df, payments_df, scores_df):
        pivot_keys_all = obs_df[["nit_enmascarado", "num_oblig_enmascarado", "fecha"]].drop_duplicates().copy()
        pivot_keys_all["pivot_month"] = pivot_keys_all["fecha"].dt.to_period("M")
        rel_oblig = pivot_keys_all[["nit_enmascarado", "num_oblig_enmascarado"]].drop_duplicates()
        obs_months = sorted(pivot_keys_all["pivot_month"].unique())

        pays_work = payments_df.merge(rel_oblig, on=["nit_enmascarado", "num_oblig_enmascarado"], how="inner")
        pays_work["year_month"] = pays_work["fecha"].dt.to_period("M")
        if "porc_pago" not in pays_work.columns:
            pays_work["porc_pago"] = 0.0
        pays_work["porc_p"] = pays_work["porc_pago"].fillna(0)

        pay_parts = []
        for month in obs_months:
            pays_before = pays_work.loc[pays_work["year_month"] < month]
            if pays_before.empty:
                continue
            agg = pays_before.groupby(["nit_enmascarado", "num_oblig_enmascarado"]).agg(
                hist_n_periodos=("porc_p", "count"),
                hist_porc_pago_med=("porc_p", "median"),
                hist_porc_pago_mean=("porc_p", "mean"),
                hist_porc_pago_std=("porc_p", "std"),
                hist_prop_pago_comp=("porc_p", lambda x: (x >= 1.0).mean()),
                hist_pago_total_med=("pago_total", "median"),
                hist_pago_ultimos3=("pago_total", lambda x: x.tail(3).mean()),
            ).reset_index()
            agg["fecha"] = month.to_timestamp()
            pay_parts.append(agg)
        pay_hist_all = pd.concat(pay_parts, ignore_index=True) if pay_parts else pd.DataFrame()
        if not pay_hist_all.empty:
            pay_hist_all["hist_porc_pago_std"] = pay_hist_all["hist_porc_pago_std"].fillna(0)

        score_parts = []
        for month in obs_months:
            sc_window = scores_df.loc[
                (scores_df["score_month"] < month) & (scores_df["score_month"] >= month - 3)
            ]
            if sc_window.empty:
                continue
            agg = sc_window.groupby(["nit_enmascarado", "num_oblig_enmascarado"]).agg(
                score_prop_hist_mean=("prob_propension", "mean"),
                score_prop_hist_std=("prob_propension", "std"),
                score_autocura_mean=("prob_auto_cura", "mean"),
                score_alrt_mean=("prob_alrt_temprana", "mean"),
                score_hist_n=("prob_propension", "count"),
            ).reset_index()
            agg["fecha"] = month.to_timestamp()
            score_parts.append(agg)
        score_hist_all = pd.concat(score_parts, ignore_index=True) if score_parts else pd.DataFrame()
        if not score_hist_all.empty:
            score_hist_all["score_prop_hist_std"] = score_hist_all["score_prop_hist_std"].fillna(0)

        df = obs_df.copy()
        if not pay_hist_all.empty:
            df = df.merge(pay_hist_all, on=["nit_enmascarado", "num_oblig_enmascarado", "fecha"], how="left")
        if not score_hist_all.empty:
            df = df.merge(score_hist_all, on=["nit_enmascarado", "num_oblig_enmascarado", "fecha"], how="left")
        return df

    def _apply_feature_engineering(self, df):
        out = df.copy()
        fecha_dt = pd.to_datetime(out["fecha"])
        out["fe_mes"] = fecha_dt.dt.month
        out["fe_trimestre"] = fecha_dt.dt.quarter
        out["fe_fin_anio"] = (fecha_dt.dt.month >= 11).astype(int)
        out["fe_inicio_anio"] = (fecha_dt.dt.month <= 2).astype(int)

        for col in ["vlr_obligacion", "vlr_vencido", "total_ing", "endeudamiento", "egresos_mes", "saldo_capital"]:
            if col in out.columns:
                out[f"log_{col}"] = np.log1p(pd.to_numeric(out[col], errors="coerce").clip(lower=0))

        out["ratio_vencido"] = out["vlr_vencido"] / (out["vlr_obligacion"] + 1)
        out["ratio_pasivos_activos"] = out["tot_pasivos"] / (out["tot_activos"] + 1)
        out["ratio_vencido_endeu"] = out["vlr_vencido"] / (out["endeudamiento"] + 1)
        out["ratio_pasivos_ing"] = (out["tot_pasivos"] / (out["total_ing"] + 1)).clip(0, 100)
        out["ratio_egr_ing"] = out["egresos_mes"] / (out["total_ing"] + 1)
        out["ratio_saldo_oblig"] = out["saldo_capital"] / (out["vlr_obligacion"] + 1)
        out["capacidad_pago"] = out["total_ing"] - out["egresos_mes"]
        out["cobertura_deuda_ingreso"] = (
            out["capacidad_pago"] / out["vlr_vencido"].replace(0, np.nan)
        ).clip(-5, 50).fillna(0)
        out["pago_x_propension"] = out["pct_meses_pagados_6m"] * out["prob_propension"]
        out["autocura_x_pago"] = out["prob_auto_cura"] * out["pct_meses_pagados_6m"]
        out["score_combinado"] = (
            out["prob_propension"] + out["prob_alrt_temprana"] + out["prob_auto_cura"]
        ) / 3
        out["alrt_x_endeu"] = out["prob_alrt_temprana"] * np.log1p(out["endeudamiento"].clip(lower=0))
        out["fe_calidad_pago"] = out["hist_porc_pago_mean"].fillna(0) * out["hist_prop_pago_comp"].fillna(0)
        out["fe_pago_consistency"] = 1 - out["hist_porc_pago_std"].fillna(1)
        out["fe_brecha_pago_score"] = out["hist_porc_pago_mean"].fillna(0) - out["prob_propension"]
        out["fe_propension_trend"] = out["prob_propension"] - out["score_prop_hist_mean"].fillna(out["prob_propension"])
        out["fe_autocura_trend"] = out["prob_auto_cura"] - out["score_autocura_mean"].fillna(out["prob_auto_cura"])
        out["tiene_alternativa"] = (out["cant_alter_posibles"] > 0).astype(int)
        out["alter_agotadas"] = (out["cant_alter_posibles"] == 3).astype(int)
        out["meses_desde_ultimo_pago"] = out["meses_desde_ultimo_pago"].clip(lower=0)
        out["edad_bin"] = pd.cut(
            out["edad_cli"],
            bins=[0, 25, 35, 45, 55, 65, 200],
            labels=["18-25", "26-35", "36-45", "46-55", "56-65", "65+"],
        ).astype(str)
        return out

    def _build_feature_frame(self, base_df, payments_df, customers_df, scores_df, is_train):
        base = self._normalize_base(base_df, is_train=is_train)
        payments = self._normalize_payments(payments_df)
        customers = self._normalize_customers(customers_df)
        scores = self._normalize_scores(scores_df)

        pivot = self._build_pivot_features(base, is_train=is_train)
        pivot = self._add_payment_recency_frequency_features(pivot, payments)
        pivot = self._add_customer_snapshot(pivot, customers)
        pivot = self._add_score_lag(pivot, scores)
        pivot = self._build_history_features(pivot, payments, scores)

        for col in self.PAY_FREQ_FEATURES + self.PAY_HISTORY_FEATURES + self.SCORE_HISTORY_FEATURES:
            if col not in pivot.columns:
                pivot[col] = np.nan
        for col in self.CUSTOMER_FEATURES + self.SCORE_FEATURES:
            if col not in pivot.columns:
                pivot[col] = np.nan

        pivot = self._apply_feature_engineering(pivot)
        return pivot

    @staticmethod
    def _split_temporal(df, validation_periods):
        periods = sorted(df["fecha"].dt.to_period("M").dropna().unique())
        if len(periods) <= validation_periods:
            raise ValueError("No hay suficientes periodos para separar train y validación")
        val_periods = set(periods[-validation_periods:])
        mask_val = df["fecha"].dt.to_period("M").isin(val_periods)
        train_df = df.loc[~mask_val].copy()
        valid_df = df.loc[mask_val].copy()
        return train_df, valid_df

    def _fit_transform_features(self, train_df, score_df=None):
        excluded_columns = {
            "target",
            "fecha",
            "id",
            "nit_enmascarado",
            "num_oblig_orig_enmascarado",
            "num_oblig_enmascarado",
            "var_rpta_alt",
        }
        feature_columns = [col for col in train_df.columns if col not in excluded_columns]
        X_train = train_df[feature_columns].copy()
        X_score = score_df[feature_columns].copy() if score_df is not None else None

        categorical_columns = X_train.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
        numeric_columns = [col for col in feature_columns if col not in categorical_columns]
        numeric_fill = X_train[numeric_columns].apply(pd.to_numeric, errors="coerce").median()

        X_train[numeric_columns] = X_train[numeric_columns].apply(pd.to_numeric, errors="coerce").fillna(numeric_fill)
        if X_score is not None:
            X_score[numeric_columns] = X_score[numeric_columns].apply(pd.to_numeric, errors="coerce").fillna(numeric_fill)

        category_maps = {}
        for col in categorical_columns:
            train_values = X_train[col].fillna("MISSING").astype(str)
            known_values = sorted(set(train_values.unique()) | {"MISSING"})
            category_maps[col] = {value: idx for idx, value in enumerate(known_values)}
            X_train[col] = train_values.map(category_maps[col]).astype(int)
            if X_score is not None:
                score_values = X_score[col].fillna("MISSING").astype(str)
                score_values = score_values.where(score_values.isin(category_maps[col]), "MISSING")
                X_score[col] = score_values.map(category_maps[col]).astype(int)

        X_train = X_train.replace([np.inf, -np.inf], np.nan).fillna(X_train.median(numeric_only=True)).astype(float)
        if X_score is not None:
            X_score = X_score.replace([np.inf, -np.inf], np.nan).fillna(X_train.median(numeric_only=True)).astype(float)

        preprocess = {
            "feature_columns": feature_columns,
            "categorical_columns": categorical_columns,
            "numeric_fill": numeric_fill.to_dict(),
            "category_maps": category_maps,
            "post_fill": X_train.median(numeric_only=True).to_dict(),
        }
        return X_train, X_score, preprocess

    def _transform_features(self, df, preprocess):
        X = df.reindex(columns=preprocess["feature_columns"]).copy()
        numeric_columns = [col for col in preprocess["feature_columns"] if col not in preprocess["categorical_columns"]]
        X[numeric_columns] = X[numeric_columns].apply(pd.to_numeric, errors="coerce")
        for col, fill_value in preprocess["numeric_fill"].items():
            if col in X.columns:
                X[col] = X[col].fillna(fill_value)

        for col in preprocess["categorical_columns"]:
            mapping = preprocess["category_maps"][col]
            values = X[col].fillna("MISSING").astype(str)
            values = values.where(values.isin(mapping), "MISSING")
            X[col] = values.map(mapping).astype(int)

        X = X.replace([np.inf, -np.inf], np.nan)
        for col, fill_value in preprocess["post_fill"].items():
            if col in X.columns:
                X[col] = X[col].fillna(fill_value)
        return X.astype(float)

    @staticmethod
    def _buscar_threshold(y_true, probas, threshold_min, threshold_max, threshold_step):
        threshold_grid = np.arange(threshold_min, threshold_max, threshold_step)
        mejor_threshold = 0.5
        mejor_f1 = -1.0
        for threshold in threshold_grid:
            pred = (probas >= threshold).astype(int)
            score = f1_score(y_true, pred, zero_division=0)
            if score > mejor_f1:
                mejor_f1 = score
                mejor_threshold = float(threshold)
        return mejor_threshold, mejor_f1

    @staticmethod
    def _ks(y_true, probas):
        positivos = np.sort(probas[y_true == 1])
        negativos = np.sort(probas[y_true == 0])
        if len(positivos) == 0 or len(negativos) == 0:
            return None
        grid = np.linspace(0, 1, max(len(positivos), len(negativos)))
        pos_interp = np.interp(grid, np.linspace(0, 1, len(positivos)), positivos)
        neg_interp = np.interp(grid, np.linspace(0, 1, len(negativos)), negativos)
        return float(np.max(np.abs(pos_interp - neg_interp)))

    @staticmethod
    def _build_estimator(config):
        if LIGHTGBM_AVAILABLE:
            return LGBMClassifier(**config.get("lightgbm_params", {})), "lightgbm"
        return XGBClassifier(**config.get("fallback_xgb_params", {})), "xgboost"

    def entrenamiento(self, df_entrenamiento, df_pagos, df_customer, df_scores, ruta, ruta_columnas, config=None):
        config = config or {}
        target_col = config.get("target_col", "var_rpta_alt")
        id_col = config.get("id_col", "id")
        id_columns = config.get("id_columns", [])
        validation_periods = int(config.get("validation_periods", 1))
        threshold_min = float(config.get("threshold_min", 0.1))
        threshold_max = float(config.get("threshold_max", 0.9))
        threshold_step = float(config.get("threshold_step", 0.02))

        feature_df = self._build_feature_frame(df_entrenamiento, df_pagos, df_customer, df_scores, is_train=True)
        feature_df[id_col] = self._construir_id(feature_df, id_col, id_columns)
        feature_df = feature_df.loc[feature_df[target_col.replace("var_rpta_alt", "target") if target_col not in feature_df.columns else target_col].notna()].copy()
        if "target" not in feature_df.columns and target_col in feature_df.columns:
            feature_df["target"] = feature_df[target_col]

        train_df, valid_df = self._split_temporal(feature_df, validation_periods=validation_periods)
        y_train = train_df["target"].astype(int)
        y_valid = valid_df["target"].astype(int)

        X_train, X_valid, preprocess = self._fit_transform_features(train_df, valid_df)
        estimator, model_name = self._build_estimator(config)
        estimator.fit(X_train, y_train)

        probas_valid = estimator.predict_proba(X_valid)[:, 1]
        threshold, f1_valid = self._buscar_threshold(y_valid, probas_valid, threshold_min, threshold_max, threshold_step)
        pred_valid = (probas_valid >= threshold).astype(int)

        full_X, _, full_preprocess = self._fit_transform_features(feature_df)
        full_y = feature_df["target"].astype(int)
        final_estimator, _ = self._build_estimator(config)
        final_estimator.fit(full_X, full_y)

        metricas = [{
            "dataset": "validation",
            "modelo": model_name,
            "accuracy": float(accuracy_score(y_valid, pred_valid)),
            "precision": float(precision_score(y_valid, pred_valid, zero_division=0)),
            "recall": float(recall_score(y_valid, pred_valid, zero_division=0)),
            "f1": float(f1_valid),
            "roc_auc": float(roc_auc_score(y_valid, probas_valid)) if y_valid.nunique() > 1 else None,
            "ks": self._ks(y_valid.to_numpy(), probas_valid),
            "threshold": float(threshold),
            "rows_train": int(len(train_df)),
            "rows_validation": int(len(valid_df)),
        }]

        artefacto = {
            "model": final_estimator,
            "preprocess": full_preprocess,
            "threshold": threshold,
            "id_col": id_col,
            "id_columns": id_columns,
            "model_name": model_name,
        }

        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        joblib.dump(artefacto, ruta)
        if ruta_columnas:
            pd.DataFrame({"feature": full_preprocess["feature_columns"]}).to_csv(ruta_columnas, index=False)
        return True, metricas

    def ejecucion(self, df_modelo, df_pagos, df_customer, df_scores, ruta, ruta_columnas, config=None):
        del ruta_columnas, config
        if not os.path.exists(ruta):
            raise FileNotFoundError(f"No existe el modelo serializado en {ruta}")

        artefacto = joblib.load(ruta)
        feature_df = self._build_feature_frame(df_modelo, df_pagos, df_customer, df_scores, is_train=False)
        feature_df[artefacto["id_col"]] = self._construir_id(feature_df, artefacto["id_col"], artefacto["id_columns"])
        X_score = self._transform_features(feature_df, artefacto["preprocess"])
        probas = artefacto["model"].predict_proba(X_score)[:, 1]
        pred = (probas >= artefacto["threshold"]).astype(int)

        return pd.DataFrame(
            {
                "ID": feature_df[artefacto["id_col"]].astype(str),
                "var_rpta_alt": pred.astype(int),
                "Prob_uno": np.round(probas, 6),
                "threshold_modelo": artefacto["threshold"],
            }
        )
