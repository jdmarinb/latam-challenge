# Data Engineer Challenge Solution

Debido a que el reto no contiene una cantidad de codigo excesiva, y para simplificar la lectura del mismose decide documentar el paso a paso de al solución en este unico archivo.

## 1. Ambiente de desarrollo


Como se indica en el reto se evaluan buenas practicas de git para el trabajo con otros desarrolladores. En pro de esto, lo primero que se hace es facilitar la configuración de un ambiente de desarrollo.

para ahcer esto simplemente despues de clonar el repositorio ejecutar:

```bash
make setup
```

### 1.1. Automatización (`Makefile`)
Para centralizar y simplificar los comandos comunes del proyecto. Provee una interfaz única (`setup`, `lint`, `test`, `clean`) para que todos los desarrolladores realicen las mismas acciones de la misma manera. Existen otras opciones como `.devcontainer`; pero estaas tienen más requerimientos `make` es una herramienta robusta, probada y agnóstica al lenguaje, presente en la mayoría de los sistemas de desarrollo. En el caso de windows normalemnte no viene instalado por defecto por lo que se peude instalar con el gestor de paqeutes `scoop install make`  o revisar la [documentación oficial](https://www.gnu.org/software/make/).


### 1.2. Entorno Virtual (`uv`)
PAra gestionar los ambiente de python y la instalación de paquetes en los mimso se usa `uv`, por su buen rendimiento y capacidad para generar archhivos de `lock` y getionar `.toml` en proyectos ma´s complejos.

### 1.3. Gestión de Dependencias (`requirements-dev.txt`)
los paqeutes propios del proyecto se declaran en `requirements.txt`. De forma independiente el archivo `requirements-dev.txt` contiene las dependencias del ambiente de desarollo, como linters y herramientas de testing adicionalmente invoca a `requirements.txt` para asegurar que el entorno de desarrollo sea un superconjunto del de producción.

### 1.4. Hooks de Git (`pre-commit`)

Automatizar la revisión y formateo del código *antes* de que sea incluido en un commit. Esto asegura que todo el código que llega al repositorio cumple con los estándares de calidad definidos.

#### 1.4.1. Linter y Formateador (`Ruff`)
Analizar el código estáticamente para detectar errores, bugs, y olores de código (`code smells`).

#### 1.4.2. Detección de Secretos (`detect-secrets`)
Prevenir que información sensible (contraseñas, API keys, etc.) sea accidentalmente incluida en el control de versiones.

#### 1.5. Commits Convencionales (`Commitizen`)
`commitizen` forza un formato estándar y legible para los mensajes de commit. Esto mejora radicalmente la legibilidad del historial de `git`, facilita la generación de changelogs y permite la automatización de versiones. **NOTA:** Un desarrollador usaría `cz commit` en lugar de `git commit` para ser guiado en la creación del mensaje.

```bash
make commit
```

### 1.6. Flujo de Git (`GitFlow` simplificado)
- **Práctica**: Se utilizará un flujo basado en `GitFlow`.
  - `main`: Contiene la versión de producción estable.
  - `develop`: Es la rama principal de desarrollo donde se integra todo el trabajo.
  - `feature/<nombre>`: Ramas para desarrollar nuevas funcionalidades, que se crean a partir de `develop`.
- **Propósito**: Mantener un historial limpio y organizado, facilitando tanto el desarrollo de nuevas características como el mantenimiento de versiones estables.


## 2. Exploración de Datos (`farmers-protest-tweets-2021-2-4.json`)

Se realizó una inspección inicial del conjunto de datos para entender su estructura y dimensiones.

### 2.1. Estructura del Archivo
- **Formato**: JSONL (JSON Lines). Cada línea del archivo es un objeto JSON independiente que representa un tweet.
- **Volumen**: El archivo contiene **117,407** registros (líneas).
- **Campos Clave para el Reto**:
    - `date`: Timestamp del tweet (ej: `2021-02-24T09:23:35+00:00`). Esencial para la **Q1**.
    - `content`: Texto del tweet. Contiene los emojis necesarios para la **Q2**.
    - `user`: Objeto que contiene el `username`. Necesario para la **Q1** (usuarios con más tweets).
    - `mentionedUsers`: Lista de objetos con los usuarios mencionados. Fundamental para la **Q3**.

### 2.2. Identificación de Registros Únicos
- Cada registro cuenta con un campo `id` numérico de 64 bits (ej: `1364506249291784198`) que identifica de forma única cada tweet.
- Adicionalmente, el campo `url` también sirve como identificador único persistente.
- **Relación Usuario-Tweet**: Un mismo usuario (identificado por `user.id` o `user.username`) puede aparecer en múltiples líneas del archivo si realizó varios tweets. Para los retos de conteo de tweets por usuario (**Q1**), se debe agrupar por el identificador del usuario, no por el ID del tweet.

### 2.3. Conclusiones de la Inspección
- El archivo es lo suficientemente grande (~400MB+) como para que una lectura completa en memoria (usando `json.load`) pueda ser ineficiente o causar problemas en entornos con recursos limitados.
- Se recomienda un enfoque de **procesamiento por streaming** (línea por línea) para optimizar el uso de memoria, especialmente para las versiones enfocadas en optimización de memoria de las funciones solicitadas.
- La anidación del usuario (`user -> username`) y de las menciones (`mentionedUsers`) requerirá una navegación cuidadosa del esquema JSON para evitar errores por campos nulos o estructuras inesperadas.


## 3. Estrategias de Optimización

Para abordar los requisitos de optimización de tiempo y memoria, se han definido las siguientes estrategias técnicas:

### 3.1. Optimización de Tiempo (`time`)
El objetivo es minimizar el tiempo total de ejecución.

- **Multiprocesamiento (`Multiprocessing`)**: Dado que el procesamiento de cada línea de JSON es independiente, se utilizará un pool de procesos para paralelizar la lectura y el parsing del archivo. Esto permite aprovechar todos los núcleos de la CPU.
- **Estructuras de Datos Eficientes**: Uso de `collections.Counter` para agregaciones rápidas y `ujson` (si está disponible) para un parsing de JSON más veloz que la librería estándar.
- **Reducción de I/O**: Lectura del archivo en bloques grandes para minimizar las llamadas al sistema.

### 3.2. Optimización de Memoria (`memory`)
El objetivo es mantener una huella de memoria (RAM) constante y baja, independientemente del tamaño del archivo.

- **Generadores e Iteradores**: Procesamiento por streaming utilizando generadores (`yield`). Nunca se carga el archivo completo ni grandes subconjuntos en memoria.
- **Lazy Evaluation**: Uso de `itertools` para procesar los datos de forma perezosa.
- **Agregación Incremental**: Mantener solo los contadores necesarios en memoria (top 10) o usar estructuras que no crezcan linealmente con el número de registros si es posible.

### 3.3. Consideraciones Específicas
- **Q1 (Fechas y Usuarios)**: Se utilizará un diccionario de contadores anidado (Fecha -> Usuario -> Cuenta) para optimizar el acceso.
- **Q2 (Emojis)**: Uso de la librería `emoji` para extraer emojis de forma eficiente sin necesidad de expresiones regulares complejas que consuman mucho tiempo.
- **Q3 (Menciones)**: Extracción directa del campo `mentionedUsers` evitando procesar el texto del tweet si la información ya viene estructurada, lo cual ahorra tiempo de cómputo significativo.

### 3.4. Escalamiento a "Miles de Millones" (Big Data Principles)
Si el volumen de datos escalara a billones de registros, las estrategias locales no serían suficientes. Se aplicarían principios avanzados de ingeniería de datos:

1.  **Optimización de Formato (Columnar Storage)**:
    - **Concepto**: Cambiar de JSONL (Row-based) a **Apache Parquet** o **Avro**.
    - **Beneficio**: Parquet permite *Predicate Pushdown* (leer solo las columnas necesarias, ej: solo `date` y `user`) y *Dictionary Encoding* para comprimir strings repetitivos (como usernames), reduciendo drásticamente el I/O y el uso de memoria.

2.  **Particionamiento y Sharding**:
    - **Concepto**: Particionar los datos físicamente en disco (ej: por `year/month/day`).
    - **Beneficio**: Permite *Partition Pruning*, donde el motor de ejecución ignora carpetas de datos que no coinciden con el rango de tiempo consultado, eliminando el escaneo total del dataset.

3.  **Probabilistic Data Structures (Sketches)**:
    - **Concepto**: Usar **HyperLogLog** para conteos de cardinalidad (Q3) o **Count-Min Sketch** para frecuencias de top-k (Q2).
    - **Beneficio**: Permite estimar los top 10 con un error mínimo controlado usando una cantidad de memoria fija y diminuta (KB en lugar de GB), ideal para flujos de datos masivos.

4.  **Data Pruning & Pre-aggregation**:
    - **Concepto**: Implementar procesos de ETL que generen *Data Cubes* o tablas de agregación horaria/diaria.
    - **Beneficio**: Las consultas de "Top 10" se vuelven instantáneas al leer agregados pre-calculados en lugar de datos crudos (Raw Data).

5.  **Zero-Copy Parsing (simdjson)**:
    - **Concepto**: Utilizar el parser `pysimdjson` en lugar del `json` estándar de Python.
    - **Aplicación Actual**: Es totalmente aplicable a este reto. Al usar instrucciones SIMD (Single Instruction, Multiple Data), el parsing de cada línea de tweet ocurre casi a la velocidad de lectura de disco, reduciendo el overhead de CPU drásticamente sin cambiar el formato de entrada (JSONL).

6.  **Optimización de Memoria via String Interning**:
    - **Concepto**: Usar `sys.intern()` para usernames repetidos.
    - **Beneficio**: Si un usuario aparece miles de veces, Python solo guarda una instancia del string en memoria, reduciendo la huella de RAM en las agregaciones (Q1 y Q3).

7.  **In-place JSON Processing**:
    - **Concepto**: No cargar todo el objeto JSON si solo necesitamos un campo.
    - **Beneficio**: Usar técnicas de búsqueda de bytes (ej: buscar la posición de `"content":`) para extraer fragmentos específicos sin instanciar el diccionario completo del tweet.


## 4. Implementación de Soluciones
