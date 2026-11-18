import pandas as pd


def unificar_columnas_equivalentes(df: pd.DataFrame) -> pd.DataFrame:
    """Une columnas ya mapeadas al mismo campo sin descartar valores."""
    resultado = pd.DataFrame(index=df.index)
    for nombre in dict.fromkeys(df.columns):
        columnas = df.loc[:, df.columns == nombre]
        if columnas.shape[1] == 1:
            resultado[nombre] = columnas.iloc[:, 0]
            continue
        columnas = columnas.replace(r"^\s*$", pd.NA, regex=True)
        valores = columnas.iloc[:, 0].copy()
        for posicion in range(1, columnas.shape[1]):
            otro = columnas.iloc[:, posicion]
            presentes = valores.notna() & otro.notna()
            conflictos = valores.loc[presentes].ne(otro.loc[presentes])
            if conflictos.any():
                raise ValueError(
                    f"Las columnas equivalentes de {nombre} discrepan en "
                    f"{int(conflictos.sum())} filas; revisar antes de cargar"
                )
            valores = valores.combine_first(otro)
        resultado[nombre] = valores
    return resultado
