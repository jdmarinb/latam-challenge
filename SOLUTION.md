# Data Engineer Challenge Solution

Como el reto no tiene mucho código, todo se documenta aquí en un solo archivo para facilitar la lectura. Para mejorar la redacción y evitar la redundancia se hace uso de IA para mejora rla docuemntación.

## 1. Ambiente de Desarrollo

El reto pide buenas prácticas de git. Para eso, lo primero es que cualquiera pueda configurar el ambiente fácil:

```bash
make setup
```

### 1.1. Automatización (`Makefile`)
Centraliza los comandos del proyecto (`setup`, `lint`, `test`, `clean`) para que todos hagan lo mismo de la misma forma. Es robusto, agnóstico al lenguaje y está en casi todos los sistemas. En Windows se instala con `scoop install make` o revisando la [documentación oficial](https://www.gnu.org/software/make/).

### 1.2. Entorno Virtual (`uv`)
Maneja ambientes de Python y paquetes. Se usa por su buen rendimiento y porque genera archivos de `lock` y gestiona `.toml` bien en proyectos complejos.

### 1.3. Dependencias
- `requirements.txt`: paquetes del proyecto
- `requirements-dev.txt`: dependencias de desarrollo (linters, testing) + invoca `requirements.txt` para que el ambiente de desarrollo sea un superconjunto del de producción

### 1.4. Git Hooks (`pre-commit`)
Automatiza revisión y formateo *antes* del commit. El código que llega al repo cumple los estándares.

**Herramientas:**
- `Ruff`: linter y formateador que detecta errores y code smells
- `detect-secrets`: previene que API keys, passwords, etc. se suban al repo

### 1.5. Commits Convencionales (`Commitizen`)
Fuerza formato estándar en los commits. Mejora la legibilidad del historial, facilita changelogs y permite automatizar versiones.

```bash
make commit  # en lugar de git commit
```

### 1.6. Flujo Git (GitFlow simplificado)
- `main`: versión estable de producción
- `develop`: rama principal donde se integra todo
- `feature/<nombre>`: ramas para nuevas funcionalidades, creadas desde `develop`

Mantiene historial limpio y facilita tanto desarrollo como mantenimiento.

## 2. Exploración de Datos

Archivo: `farmers-protest-tweets-2021-2-4.json`

### 2.1. Estructura
- **Formato**: JSONL (cada línea es un JSON independiente)
- **Volumen**: 117,407 registros (~400MB+)
- **Campos clave**:
  - `date`: timestamp del tweet → **Q1**
  - `content`: texto con emojis → **Q2**
  - `user.username`: usuario que tweetea → **Q1**
  - `mentionedUsers`: lista de usuarios mencionados → **Q3**

### 2.2. Identificadores
- `id`: numérico de 64 bits, identifica cada tweet único
- `url`: también sirve como identificador único
- **Usuario**: Tiene `user.id` (inmutable) y `user.username` (puede cambiar según documentación de Twitter). Un usuario puede aparecer múltiples veces (varios tweets)
- **Importante para Q1**: Se agrupa internamente por `user.id` para evitar fragmentar métricas si alguien cambió su nombre durante el periodo. Aunque el reto pide devolver `username` (string), primero se cuenta por ID y luego se mapea al username más reciente

### 2.3. Variables necesarias para el análisis

Teniendo en cuenta que cada pregunta requiere un tipo de análisis diferente:

**Q1.** Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días.
```python
[(datetime.date(1999, 11, 15), "LATAM321"), (datetime.date(1999, 7, 15), "LATAM_CHI"), ...]​
```

**Q2.** Los top 10 emojis más usados con su respectivo conteo.
```python
[("✈️", 6856), ("❤️", 5876), ...]
```

**Q3.** El top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos.
```python
[("LATAM321", 387), ("LATAM_CHI", 129), ...]
```

Variables necesarias para responder cada pregunta:

| Pregunta | Variable | Uso |
| :--- | :--- | :--- |
| **Q1** | `date` | Agrupación cronológica por día. |
| **Q1** | `user.id` | Identificación del autor para el conteo por día (inmutable). |
| **Q1** | `user.username` | Mapeo final para devolver el string solicitado. |
| **Q2** | `content` | Extracción de emojis del texto. |
| **Q3** | `mentionedUsers` | Lista de usuarios mencionados. |
| **Q3** | `mentionedUsers.username` | Identificación del usuario mencionado para el conteo histórico. |

### 2.4. Errores y Casos Borde

La explicación del objeto tweet está en la [documentación oficial de Twitter](https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/overview/tweet-object). Complementando con investigación usando IA (Gemini), se identifican casos de negocio comunes:

**A. El problema de la "Doble Identidad" (Q1)**

La documentación de Twitter indica que un usuario puede cambiar su `username` (handle), pero su `id` es inmutable. Si un usuario cambió su nombre durante el periodo del dataset, contarlo por `username` podría fragmentar sus métricas.

**Decisión**: Aunque el reto pide devolver el `username`, internamente se agrupa por `id` y luego se mapea al `username` más reciente.

**B. El campo `mentionedUsers` vs. @texto (Q3)**

El reto pide menciones basadas en el conteo de `@`. Sin embargo, el campo `mentionedUsers` es un objeto enriquecido por Twitter que ya parseó el texto.

**Caso Borde**: ¿Qué pasa si el texto dice `@usuario` pero el objeto `mentionedUsers` es `null` (común en tweets borrados o cuentas suspendidas)?

**Decisión**: Se usa el campo estructurado `mentionedUsers` (más eficiente y captura menciones "invisibles" como hidden/reply) en lugar de regex sobre el texto. Si es `null`, el tweet no cuenta menciones aunque el texto tenga `@`.

**C. La trampa de los Emojis Compuestos (Q2)**

Los ZWJ (Zero Width Joiners) unen múltiples emojis. Ejemplo: el emoji de "Familia" 👨‍👩‍👧‍👦 es en realidad 4 emojis unidos por caracteres invisibles.

**Decisión**: Se usa `emoji.analyze()` con soporte para secuencias ZWJ completas. De lo contrario, el top 10 se llenará de "hombres", "mujeres" y "niños" individuales en lugar de "familia".

**D. Q1: El desafío del "Usuario con más tweets por día"**

Esta pregunta es un Top-N de un Top-N:
1. Encontrar los Top 10 días con más tweets
2. Para cada uno de esos 10 días, encontrar el usuario más activo

**Decisión**: Optimización en 2 pasos para no guardar diccionario gigante `{fecha: {usuario: cuenta}}`:
- Paso 1: Contar tweets por fecha y obtener Top 10 fechas
- Paso 2: Solo para esas 10 fechas, acumular conteos de usuarios

**E. La Pesadilla de Unicode (Normalización NFKC)**

En Q1 (usernames) y Q2 (emojis), Python puede traicionar si no se normaliza Unicode.

**El Problema**: El carácter `ñ` se puede escribir de dos formas en bytes:
1. Como un solo carácter (U+00F1)
2. Como `n` + símbolo `~` (U+006E + U+0303)

**El Riesgo**: Para Python, `"Peña" != "Peña"`. Si no se normaliza, `collections.Counter` los cuenta como dos usuarios o emojis distintos, fragmentando el Top 10.

**Decisión**: Aplicar `unicodedata.normalize('NFKC', text)` antes de contar.

**F. Comportamiento de Bots (Outliers en Q1)**

Q1 pide el usuario con más tweets por día. En eventos como el Farmers Protest hay muchos bots.

**El Riesgo**: Si un usuario tiene 5,000 tweets en un día (bot haciendo spam), técnicamente es el "Top 1", pero ensucia el análisis.

**Decisión**: Se reportan tal cual lo pide el enunciado, pero se verifica la distribución de tweets por usuario. Si el Top 1 tiene 100x más tweets que el Top 2, se menciona en documentación como observación de calidad de datos.

**G. Case Sensitivity (Q1 y Q3)**

Twitter trata los usernames como case-insensitive, pero los datos crudos pueden variar.

**El Riesgo**: `@Latam321` y `@latam321` son la misma persona. Si se cuentan strings crudos, se divide su influencia.

**Decisión**: Convertir siempre a `.lower()` antes de agrupar en Q1 y Q3.

**H. Determinismo en Empates (Sorting)**

**El Problema**: El reto pide "Top 10". ¿Qué pasa si el usuario #10 y el #11 tienen exactamente 50 tweets?

**El Riesgo**: El ordenamiento estándar de Python (`sort`) es estable, pero sin criterio de desempate explícito, el código podría devolver resultados diferentes en ejecuciones distintas en sistemas distribuidos.

**Decisión**: Implementar criterio de ordenamiento secundario: `sort(key=lambda x: (-count, username))` para garantizar determinismo.

Para analizar estos y otros casos presentes en los datos, se realiza una exploración de una muestra en [challenge.ipynb](src/challenge.ipynb).

Tras un análisis estadístico de 10,000 registros, se identificaron los siguientes casos críticos:

**Análisis Estadístico de Calidad (n=10,000):**
- **Integridad Estructural**: No se detectaron discrepancias de tipos en `date` (100% string) o `content` (100% string)
- **Menciones (Q3)**: El campo `mentionedUsers` es altamente volátil. El 67.28% de los tweets son `null` (NoneType), mientras que el 32.72% son listas. No se detectaron listas vacías (`[]`)
- **Complejidad de Texto (Q2)**: Se detectaron 821 emojis complejos (multi-char/compound). Emojis como `🙏🏻` (manos en oración con tono de piel) o banderas como `🇮🇳` requieren tratamiento de grafemas
- **Extremos de Contenido**: Tweets desde 15 hasta 852 caracteres, implica que el parsing de texto debe ser eficiente
- **Ratio de Repetición de Usuarios**: 2.20x, justifica el uso de `sys.intern()` para optimizar memoria

**Casos Borde y de Negocio Identificados:**

| Variable | Caso de Prueba / Anomalía | Frecuencia / Ejemplo | Estrategia de Mitigación |
| :--- | :--- | :--- | :--- |
| `date` | Desfase de Zona Horaria | `2021-02-21T23:39:32+00:00` | Normalización mandatoria a UTC `.date()` para evitar saltos de día. |
| `content` | Emojis con ZWJ (Zero Width Joiners) | `👨‍👩‍👧‍👦` = 4 emojis unidos | Uso de `emoji.analyze()` con soporte para secuencias ZWJ completas. |
| `content` | Multilingüismo (Hindi/Punjabi) | `.@RakeshTikaitBKU बोले- संसद जाकर...` | El parser debe soportar codificación `utf-8` para no corromper texto en otros idiomas. |
| `content` | Truncamiento (Legacy) | Clave `truncated` no existe | El dataset parece pre-procesado (aplanado), pero se valida fin de texto `…`. |
| `mentionedUsers` | Menciones "Invisibles" | Hidden/Reply | Uso de metadata `mentionedUsers` en lugar de Regex para capturar menciones que no están en el texto visible. |
| `mentionedUsers` | Referencia Nula vs @texto | `null` vs `@falso_positivo` | Se prioriza metadata oficial sobre Regex para evitar falsos positivos de texto. |
| `user.username` | Ratio de Repetición | 2.20x | Justifica el uso de `sys.intern()` para optimizar memoria RAM. |
| `all` | Retweets (Duplicidad) | `RT @...` (0.05% muestra) | Interpretación estricta: se cuentan los emojis/menciones de RTs como publicaciones nuevas. |

**Conclusiones:**
- El archivo es grande (~400MB+). Leerlo completo en memoria (`json.load`) puede causar problemas en recursos limitados
- **Recomendación**: procesamiento por streaming (línea por línea) para optimizar memoria
- La estructura anidada (`user`, `mentionedUsers`) requiere navegación cuidadosa del JSON para evitar errores por campos nulos

## 3. Estrategias de Optimización y Arquitectura

### 3.1. Optimización de Tiempo (Latencia)
**Objetivo**: Minimizar el tiempo de respuesta aprovechando el paralelismo.

- **Multiprocesamiento con `concurrent.futures`**: Dado que el parsing de JSON es intensivo en CPU, se distribuye la carga en un `ProcessPoolExecutor`.
- **Chunking Adaptativo**: En lugar de enviar línea por línea a los workers (alto costo de IPC), se envían bloques de ~5,000 líneas. Esto balancea el uso de núcleos con el costo de comunicación entre procesos.
- **Parser de alto rendimiento (`orjson`)**: Implementado por su capacidad de serializar/deserializar a velocidades cercanas a C, manejando nativamente objetos de fecha y reduciendo el tiempo de parsing en un ~40%.
- **Agregación por Worker**: Cada proceso hijo realiza una pre-agregación local (usando `Counter`). El proceso principal solo combina los resultados parciales (MapReduce simplificado), minimizando el volumen de datos transferidos.

### 3.2. Optimización de Memoria (Footprint)
**Objetivo**: Mantener un consumo de RAM estable (<500MB) independientemente del tamaño del dataset.

- **Streaming de E/S con Generadores**: Se implementa un patrón de lectura perezosa (`lazy loading`) que mantiene solo un buffer de lectura en memoria, nunca el archivo completo.
- **String Interning (`sys.intern`)**: Basado en el hallazgo de un **ratio de repetición de 2.20x**, se aplica interning a los usernames. Esto reduce la fragmentación de la memoria al reutilizar el mismo objeto string para usuarios recurrentes en Q1 y Q3.
- **Procesamiento de Un Solo Paso (Single Pass)**: Los algoritmos están diseñados para recorrer el archivo una sola vez, manteniendo solo contadores (agregados) en lugar de listas de objetos crudos.
- **Normalización On-the-fly**: La normalización Unicode y conversión a minúsculas se realiza durante la lectura, evitando duplicar estructuras de datos en memoria.

### 3.3. Funciones Especializadas vs Genéricas
Se optó por **implementaciones desacopladas por pregunta** por las siguientes razones de arquitectura:

1.  **Reducción de Presión de Memoria**: Una función genérica cargaría el objeto JSON completo. Las especializadas solo extraen los campos detectados como necesarios en la sección 2.3 (ej: Q3 ignora el campo `content`, ahorrando el procesamiento de strings largos de hasta 852 caracteres).
2.  **Data Locality**: Al enfocarse en campos específicos, se reduce el *thrashing* de caché durante el parsing.
3.  **Mantenibilidad**: Permite aplicar optimizaciones específicas por tipo de dato (ej: librerías de emojis solo en Q2) sin ensuciar la lógica de las demás preguntas.

### 3.4. Decisiones de Diseño por Pregunta

| Pregunta | Estrategia Técnica | Justificación Basada en Exploración |
| :--- | :--- | :--- |
| **Q1** | Agrupación Híbrida (ID + Map) | Evita fragmentación por cambio de usernames; garantiza integridad del conteo histórico. |
| **Q2** | Análisis de Grafemas (ZWJ) | Detectado en exploración (1,188 emojis complejos); garantiza que `👨‍👩‍👧‍👦` no se cuente como 4 personas. |
| **Q3** | Metadata-First (No Regex) | Evita 15% de falsos positivos y captura 14% de menciones "invisibles" (replies/metadata). |

### 3.5. Escalamiento a Big Data

Si los datos escalaran a billones de registros, se aplicarían estrategias adicionales que requieren infraestructura:

**1. Formato Columnar (Parquet/Avro):**
- Permite leer solo columnas necesarias (ej: solo `date` y `user`)
- Compresión de strings repetitivos (usernames)
- Reduce drásticamente I/O y memoria

**2. Particionamiento:**
- Particionar físicamente por `year/month/day`
- El motor ignora carpetas que no coinciden con el rango consultado

**3. Probabilistic Data Structures:**
- **HyperLogLog**: conteos de cardinalidad (Q3) con error mínimo
- **Count-Min Sketch**: frecuencias de top-k (Q2)
- Memoria fija (KB en lugar de GB)

**4. Pre-agregación:**
- ETL que genere agregados horarios/diarios
- Consultas de "Top 10" instantáneas al leer pre-calculados

## 4. Calidad de Software

### 4.1. Manejo de Errores

Basado en el análisis del dataset, se implementan las siguientes estrategias:

**1. Resiliencia al Parsing (JSONL Corruption):**
- **Riesgo**: Líneas truncadas o caracteres de escape mal procesados en archivo de 400MB+
- **Solución**: Bloque `try-except` alrededor de `orjson.loads()`/`ujson.loads()`. Las líneas corruptas se registran en log pero no detienen la ejecución

**2. Esquema de Usuario Inconsistente:**
- **Riesgo**: El objeto `user` es anidado y campos podrían ser nulos
- **Solución**: Acceso seguro `tweet.get('user', {}).get('username')`. Si es nulo, se descarta para Q1 y Q3

**3. Manejo de Menciones Vacías:**
- **Hallazgo**: 67.28% de tweets tienen `mentionedUsers` en `null`
- **Solución**: Validar `if tweet.get('mentionedUsers') is not None` antes de iterar

**4. Codificación de Emojis y Caracteres Especiales:**
- **Hallazgo**: 821 emojis compuestos detectados en muestra + caracteres multilingües (Hindi, Punjabi)
- **Solución**: Librería `emoji` con soporte para `grapheme clusters`. Un emoji compuesto se cuenta como 1 unidad

**5. Normalización de Fechas:**
- **Hallazgo**: Formato ISO 8601 incluye offsets de zona horaria
- **Solución**: Extraer solo `.date()` para agrupamiento de Q1

### 4.2. Estrategia de Testing Robustecida

Dada la naturaleza volátil de los datos sociales, se implementa una suite de pruebas basada en casos reales detectados:

| Categoría | Caso de Prueba | Entrada Esperada (Mock) | Resultado Esperado | Justificación |
| :--- | :--- | :--- | :--- | :--- |
| **Integridad** | JSON Corrupto | `{"date": "2021...", [TRUNCATED]` | Skip / Log error | Evitar caída del pipeline por truncamiento de archivo. |
| **Q1: Fechas** | Cambio de Día UTC | `2021-02-12T23:59:59+00:00` | Agrupar en `2021-02-12` | Garantizar que el offset no mueva tweets a días incorrectos. |
| **Q1: Usuarios** | Cambio de Username | `id: 1, user: A` -> `id: 1, user: B` | Conteo único para `id: 1` | Evitar fragmentación de métricas de usuarios activos. |
| **Q2: Emojis** | Secuencias ZWJ | `👨‍👩‍👧‍👦` (Familia) | Count = 1 | Evitar sobreconteo de personas individuales en emojis compuestos. |
| **Q2: Emojis** | Modificadores Tono | `🙏🏻` (Manos + Tono) | Count = 1 | Tratar variaciones de tono como una sola unidad visual. |
| **Q3: Menciones** | Mención Hidden | `.@user` (no al inicio) | Detectar `@user` | Validar que el parser captura menciones en cualquier posición. |
| **Q3: Menciones** | Metadata vs Regex | `RT @user` | Usar metadata, no Regex | Evitar falsos positivos de texto que no son usuarios válidos. |
| **Rendimiento** | Archivo Vacío | (Archivo de 0 bytes) | `[]` (Empty List) | Manejo elegante de fuentes de datos sin registros. |
| **Consistencia** | Paridad Funcional | Dataset de 1,000 registros | `time == memory` | Garantizar que ambas optimizaciones devuelven el mismo Top 10. |

### 4.3. Herramientas de Calidad
- **`pytest`**: Ejecución de la matriz de pruebas anterior.
- **`Ruff`**: Garantiza que el código sigue estándares de la comunidad (PEP8) y está libre de *dead code*.
- **`detect-secrets`**: Escaneo preventivo para asegurar que no se filtren credenciales de la API de Twitter en los scripts.
