# InfluxDB Watcher - Surveillance des défauts

Ce script Go surveille en temps réel les variables InfluxDB pour détecter et enregistrer les défauts dans MySQL.

## Description

Le watcher surveille les champs suivants depuis InfluxDB :
- **SEQ01** (Variateur AFE) : `SEQ01.OLI.A.IC1` et `SEQ01.OLI.A.PC1`
- **SEQ02** (Variateur HB1) : `SEQ02.OLI.A.IC1` et `SEQ02.OLI.A.PC1`
- **SEQ03** (Variateur HB2) : `SEQ03.OLI.A.IC1` et `SEQ03.OLI.A.PC1`
- **SEQ12** (HC1 PDC1) : `SEQ12.OLI.A.IC1` et `SEQ12.OLI.A.PC1`
- **SEQ22** (HC1 PDC2) : `SEQ22.OLI.A.IC1` et `SEQ22.OLI.A.PC1`
- **SEQ13** (HC2 PDC3) : `SEQ13.OLI.A.IC1` et `SEQ13.OLI.A.PC1`
- **SEQ23** (HC2 PDC4) : `SEQ23.OLI.A.IC1` et `SEQ23.OLI.A.PC1`
- **SEQ14** (HC3 PDC5) : `SEQ14.OLI.A.IC1` et `SEQ14.OLI.A.PC1`
- **SEQ24** (HC3 PDC6) : `SEQ24.OLI.A.IC1` et `SEQ24.OLI.A.PC1`

Lorsqu'une valeur change, le script :
1. Décode la valeur en binaire
2. Identifie les bits actifs (à 1)
3. Mappe chaque bit à son défaut correspondant
4. Insère chaque défaut dans la table MySQL `kpi_defauts`

## Configuration

### Variables d'environnement

Vous pouvez configurer le script via des variables d'environnement :

```bash
# InfluxDB
export INFLUX_HOST="tsdbe.nidec-asi-online.com"
export INFLUX_PORT="443"
export INFLUX_USER="nw"
export INFLUX_PW="at3Dd94Yp8BT4Sh!"
export INFLUX_DB="signals"
export INFLUX_MEAS="fastcharge"
export INFLUX_TAG_PROJECT="project"

# MySQL
export MYSQL_DSN="AdminNidec:u6Ehe987XBSXxa4@tcp(141.94.31.144:3306)/indicator?parseTime=true"
```

Si les variables ne sont pas définies, les valeurs par défaut seront utilisées.

## Installation

### Prérequis
- Go 1.21 ou supérieur

### Installation des dépendances

```bash
cd influx-watcher
go mod download
```

## Compilation

```bash
go build -o influx-watcher main.go
```

## Exécution

### Mode développement
```bash
go run main.go
```

### Mode production
```bash
./influx-watcher
```

### Avec Docker (optionnel)
```bash
docker build -t influx-watcher .
docker run -d --name influx-watcher \
  -e INFLUX_HOST="tsdbe.nidec-asi-online.com" \
  -e INFLUX_PW="at3Dd94Yp8BT4Sh!" \
  influx-watcher
```

## Fonctionnement

Le script fonctionne en boucle avec un intervalle de 30 secondes (configurable) :
1. Interroge InfluxDB pour obtenir les dernières valeurs des champs surveillés
2. Compare avec les valeurs précédentes
3. Si une valeur a changé et contient des bits actifs :
   - Décode chaque bit actif
   - Récupère la description du défaut depuis le mapping
   - Insère dans MySQL avec :
     - `site` : nom du site (tag project d'InfluxDB)
     - `date` : date et heure actuelles
     - `defaut` : description du défaut
     - `eqp` : nom de l'équipement (Variateur AFE, HB1, HC1, etc.)

## Structure de la table MySQL

```sql
CREATE TABLE kpi_defauts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    site VARCHAR(50) NOT NULL,
    date DATETIME NOT NULL,
    defaut VARCHAR(100) NOT NULL,
    eqp VARCHAR(100) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Logs

Le script affiche des logs pour chaque défaut détecté :
```
2024/11/14 10:30:15 Défaut détecté - Site: Site1, Eqp: Variateur AFE, Défaut: IC00 - 080Q1 - [NX_OBI_400VAC_Q_Flt] - 400VAC - CB Open
```

## Exemple de décodage binaire

Si `SEQ01.OLI.A.IC1 = 128` (décimal) :
- Binaire : `10000000`
- Bit 7 est actif
- Défaut correspondant : IC07 - 110Q1 - [UPS_OBI_OUT230VAC_Q_Flt]
- Insertion dans MySQL :
  ```
  site='Site1', date='2024-11-14 10:30:15', defaut='IC07 - 110Q1...', eqp='Variateur AFE'
  ```

Si plusieurs bits sont actifs (ex: 129 = bits 0 et 7), deux lignes seront insérées.

## Maintenance

- **Modifier l'intervalle de surveillance** : Changer la variable `pollInterval` dans `main.go`
- **Ajouter de nouveaux champs** : Ajouter une entrée dans `equipConfigs`
- **Modifier les mappings** : Éditer les maps `IC1_SEQxx_MAP` et `PC1_SEQxx_MAP`
