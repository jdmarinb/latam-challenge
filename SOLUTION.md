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
