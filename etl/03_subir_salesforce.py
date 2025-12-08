# Leer archivo limpio
df = pd.read_excel(
    "salida/Ficha_Social_v2.xlsx",
    dtype={
        "Número de Documento": str,
        "Número de Documento.1": str,
        "Número de documento del niño": str
    }
)

# Renombrar columnas → Salesforce
df = df.rename(columns=mapeo)

# Convertir todo a string excepto Marca_temporal__c
for col in df.columns:
    if col != "Marca_temporal__c":
        df[col] = df[col].astype(str)

# Limpiar NaN
df = df.replace({np.nan: None})

records = df.to_dict("records")

bulk = SalesforceBulk(
    username=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    security_token=os.environ["SF_TOKEN"],
    sandbox=True
)

job = bulk.create_insert_job("Ficha_social_e__c", contentType='JSON')

batch_size = 1000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    json_data = json.dumps(batch, ensure_ascii=False)
    bulk.post_batch(job, BytesIO(json_data.encode("utf-8")))
    print(f"Lote {i//batch_size + 1} enviado ({len(batch)} registros)")

bulk.close_job(job)

print("✅ ETL completado y enviado a Salesforce.")
