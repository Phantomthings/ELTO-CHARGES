package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// Configuration InfluxDB
var (
	influxHost       = getEnv("INFLUX_HOST", "tsdbe.nidec-asi-online.com")
	influxPort       = getEnv("INFLUX_PORT", "443")
	influxUser       = getEnv("INFLUX_USER", "nw")
	influxPw         = getEnv("INFLUX_PW", "at3Dd94Yp8BT4Sh!")
	influxDB         = getEnv("INFLUX_DB", "signals")
	influxMeas       = getEnv("INFLUX_MEAS", "fastcharge")
	influxTagProject = getEnv("INFLUX_TAG_PROJECT", "project")

	// Configuration MySQL
	mysqlDSN = getEnv("MYSQL_DSN", "AdminNidec:u6Ehe987XBSXxa4@tcp(141.94.31.144:3306)/indicator?parseTime=true")

	// Intervalle de polling (en secondes)
	pollInterval = 30
)

// Mappings pour les codes d'erreur
var (
	IC1_SEQ01_MAP = map[int]string{
		0:  "IC00 - 080Q1 - [NX_OBI_400VAC_Q_Flt] - 400VAC - CB Open",
		1:  "IC01 - 080Q1 - [NX_OBI_400VAC_ElecFLT] - 400VAC - Electrical Fault Flt",
		2:  "IC02 - 081Q1-081F1 - [NX_OBI_LIGHTARR_FLT] - NX PARAFOUDRE Flt",
		3:  "IC03 - [CU_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Cooling unit Open",
		4:  "IC04 - [AUX_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Open",
		5:  "IC05 - [UPS_OBI_IN230VAC_Q_Flt] - 230VAC Main circuit breakers UPS INPUT Open",
		6:  "IC06 - [IMD_IBH_230VAC_Q] - 230VAC circuit breakers IMD Open",
		7:  "IC07 - 110Q1 - [UPS_OBI_OUT230VAC_Q_Flt] - 230VAC Main circuit breakers UPS OUT Open",
		8:  "IC08 - [REL_OBI_230VAC_Q_Flt] - 230VAC UPS Relay Circuit breaker Open",
		9:  "IC09 - [IMD_OBI_230VAC_Q_Flt] - 230VAC circuit breakers IMD Open",
		10: "IC10 - [LOV_AU_OBI_230VAC_Q_Flt] - 230VAC circuit breakers Measurement + AU Open",
		14: "IC14 - 105Q1 - [EXTGAZ_OBI_400VAC_Q_Flt] - 400VAC Circuit breaker GAZ Extractor Open",
		15: "IC15 - 105Q2 - [EXTGAZ_OBI_K] - Gaz Extractor RUN Open",
		16: "IC16 - 159Q1 - [AFE1_OBI_ACPL_F_FLT] - AFE1 AC Preload fuses - Fuse FLT Open",
		17: "IC17 - 193KM1 - [AFE1_OBI_AC_Q_Flt] - AFE1 AC Circuit breaker",
		18: "IC18 - 193K1 - [AFE1_OBI_ACPL_K] - AFE1 DC Preload contactor",
		19: "IC19 - [AMS_OBI_AFE1_ComFlt] AFE1 Loss communication",
		20: "IC20 - [IMD_OBI_DefCom] Loss communication",
		21: "IC21 - [AMS_OBI_RIO_ComFlt] RIO Loss communication",
		22: "IC22 - [AMS_OBI_Lovato_ComFlt] Lovato Loss communication",
		23: "IC23 - [AMS_OBI_CU_ComFlt] Loss communication",
		24: "IC24 - AFE VDC Measurement < 490VDC",
	}

	PC1_SEQ01_MAP = map[int]string{
		0:  "PC00 - 080Q1 - [NX_OBI_400VAC_Q_Flt] - 400VAC - CB Open",
		1:  "PC01 - 080Q1 - [NX_OBI_400VAC_ElecFLT] - 400VAC - Electrical Fault Flt",
		2:  "PC02 - 081Q1-081F1 - [NX_OBI_LIGHTARR_FLT] - NX PARAFOUDRE Flt",
		3:  "PC03 - [CU_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Cooling unit Open",
		4:  "PC04 - [AUX_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Open",
		5:  "PC05 - 110Q1 - [UPS_OBI_OUT230VAC_Q_Flt] - 230VAC Main circuit breakers UPS OUT Open",
		6:  "PC06 - [REL_OBI_230VAC_Q_Flt] - 230VAC UPS Relay circuit breaker Open",
		7:  "PC07 - 112Q2 - [IMD_OBI_230VAC_Q_Flt] - 230VAC circuit breakers IMD Open",
		8:  "PC08 - [NX_OBI_400VAC_DecouplingFLT] - AV Relay Decoupling Flt",
		9:  "PC09 - 351S1 - [BP_OBI_AU_EXT_FLT] - ES Push button External Open",
		10: "PC10 - 105Q1 - [EXTGAZ_OBI_400VAC_Q_Flt] - 400VAC Circuit breaker GAZ Extractor Open",
		11: "PC11 - 105Q2 - [EXTGAZ_OBI_K] - Gaz Extractor RUN Open",
		12: "PC12 - 24VDC - IO_IBH_24VDC_ON / SAFE_IBH_24VDC_ON / GAPA_IBH_24VDC_ON - 24VDC UPS Voltage ON",
		13: "PC13 - [AMS_OBI_AFE1_ComFlt] AFE1 Loss communication",
		14: "PC14 - 155F1 - [IMD_OBI_Flt] - Controleur permanent d'isolement Flt",
		15: "PC15 - [AMS_OBI_RIO_ComFlt] RIO Loss communication",
		16: "PC16 - [AMS_OBI_Lovato_ComFlt] Lovato Loss communication",
		17: "PC17 - [AMS_OBI_CU_ComFlt] Loss communication",
		18: "PC18 - 193K1 - [AFE1_OBI_ACPL_K_DiscKM] - AFE1 DC Preload contactor",
		19: "PC19 - 193K1 - [AFE1_OBI_ACPL_K_DiscKM] - AFE1 DC Preload contactor (dup.)",
		20: "PC20 - 193KM1 - [AFE1_OBI_AC_K_DiscKM] - Discordance AFE DC Line contactor",
		21: "PC21 - AFE1 FAULT",
		22: "PC22 - AFE1 voltage input flt",
		23: "PC23 - AFE1 tension < 150VDC",
		24: "PC24 - 155F2 - [SAFE_OBI_ON_FLT] - ES LIVE Flt",
		25: "PC25 - 155F1 - [IMD_OBI_Flt] - Controleur permanent d'isolement Communication fault",
		26: "PC26 - 1505Q1 - [FIRE_OBI_Q_Flt / FIRE_OBI_GazRelease_FLT] - alarme incendie",
		27: "PC27 - Conteneur température fault",
		28: "PC28 - Fire System Fault",
		29: "PC29 - Manual Force STOP",
		30: "PC30 - SEQ TimeOut STEP",
		31: "PC31 - Doors open",
	}

	IC1_SEQ02_MAP = map[int]string{
		0: "IC00 - BBMS1 No Relay closed",
		1: "IC01 - 111Q2 - [HB1_IBH_230VAC_Q] - 230VAC UPS HB1 circuit breaker Hacheur BATT1 Open",
		2: "IC02 - Manual disable HB",
		3: "IC03 - SEQ01 [SEQ01.OLI.Branch_01] AFE - Is Step 100",
	}

	PC1_SEQ02_MAP = map[int]string{
		0:  "PC00 - 360F1 - SAFE_OBI_ON_FLT - ES LIVE Flt",
		1:  "PC01 - [AMS_OBI_RIO_ComFlt] - RIO Loss communication",
		2:  "PC02 - SEQ01 - AFE - Is Step 100",
		3:  "PC03 - [AMS_OBI_HB1_ComFlt] Hacheur1 Loss communication",
		4:  "PC04 - BBMS1 - Loss Communication",
		5:  "PC05 - BBMS1 Fault",
		6:  "PC06 - HB1 - FAULT",
		7:  "PC07 - 206Q1 - [HB1_OBI_OUTDC_K_ICPCFlt] - HB1 OUT DC Contactor running condition missing",
		8:  "PC08 - 111Q2 - [HB1_IBH_230VAC_Q] - 230VAC UPS HB1 circuit breaker Hacheur BATT1",
		9:  "PC09 - 111Q5 - [HB_IBH_Measure230VAC_Q] - 230VAC Circuit breaker - Measurement circuit",
		10: "PC10 - 24VDC UPS BBMS Voltage ON Open",
		11: "PC11 - 24VDC UPS Rack1_2 Voltage ON Open",
		12: "PC12 - HB1 BATT1 Reactor temperature Fault",
		13: "PC13 - 210K1 - [GAPA_OBI_24VDC_ON_ICPCFlt] - GAPA 24V VDC Contactor running condition missing",
		14: "PC14 - BBMS1 No Relay closed",
		16: "PC16 - SEQ02 TimeOut - TimeOut 10",
		17: "PC17 - SEQ02 TimeOut - TimeOut 20",
		18: "PC18 - SEQ02 TimeOut - TimeOut 30",
		19: "PC19 - SEQ02 TimeOut - TimeOut 40",
		20: "PC20 - SEQ02 TimeOut - TimeOut 60",
	}

	IC1_SEQ03_MAP = map[int]string{
		0: "IC00 - BBMS2 No Relay closed",
		1: "IC01 - 111Q3 - [HB2_IBH_230VAC_Q] - 230VAC UPS HB2 circuit breaker Hacheur BATT2 Open",
		2: "IC02 - Manual disable HB",
		3: "IC03 - SEQ01 [SEQ01.OLI.Branch_01] AFE - Is Step 100",
	}

	PC1_SEQ03_MAP = map[int]string{
		0:  "PC00 - 360F1 - SAFE_OBI_ON_FLT - ES LIVE Flt",
		1:  "PC01 - [AMS_OBI_RIO_ComFlt] - RIO Loss communication",
		2:  "PC02 - SEQ01 - AFE - Is Step 100",
		3:  "PC03 - [AMS_OBI_HB2_ComFlt] Hacheur2 Loss communication",
		4:  "PC04 - BBMS2 - Loss Communication",
		5:  "PC05 - BBMS2 Fault",
		6:  "PC06 - HB2 - FAULT",
		7:  "PC07 - 246Q1 - [HB2_OBI_OUTDC_K_ICPCFlt] - HB2 OUT DC Contactor running condition missing",
		8:  "PC08 - 111Q3 - [HB2_IBH_230VAC_Q] - 230VAC UPS HB2 circuit breaker Hacheur BATT2",
		9:  "PC09 - 111Q5 - [HB_IBH_Measure230VAC_Q] - 230VAC Circuit breaker - Measurement circuit",
		10: "PC10 - 24VDC UPS BBMS Voltage ON Open",
		11: "PC11 - 24VDC UPS Rack1_2 Voltage ON Open",
		12: "PC12 - HB2 BATT2 Reactor temperature Fault",
		13: "PC13 - 210K1 - [GAPA_OBI_24VDC_ON_ICPCFlt] - GAPA 24V VDC Contactor running condition missing",
		14: "PC14 - BBMS2 No Relay closed",
		16: "PC16 - SEQ03 TimeOut - TimeOut 10",
		17: "PC17 - SEQ03 TimeOut - TimeOut 20",
		18: "PC18 - SEQ03 TimeOut - TimeOut 30",
		19: "PC19 - SEQ03 TimeOut - TimeOut 40",
		20: "PC20 - SEQ03 TimeOut - TimeOut 60",
	}

	PDC_SEQ_IC_MAP = map[int]string{
		0:  "IC00 - Main sequence running",
		1:  "IC01 - Ev contactor not closed",
		2:  "IC02 - No over temp Self",
		6:  "IC06 - EndPoint OCPP Connected",
		7:  "IC07 - HMI communication Fault",
		8:  "IC08 - Not charging",
		9:  "IC09 - DCBM Fault",
		10: "IC10 - Unavailable from CPO",
		11: "IC11 - Payter Com Fault",
		12: "IC12 - ZMQ Com Fault",
	}

	PDC_SEQ_PC_MAP = map[int]string{
		0:  "PC00 - RIO COM",
		2:  "PC02 - Inverter M1 Ready",
		3:  "PC03 - UpstreamSequence no fault",
		4:  "PC04 - Ev contactor no discordance",
		6:  "PC06 - No over temp Self",
		7:  "PC07 - No TO",
		8:  "PC08 - Plug no Over Temp CCS",
		9:  "PC09 - Inverter OverVoltage",
		12: "PC12 - Communication EVI",
		13: "PC13 - ES EVI",
		14: "PC14 - Manual Indispo",
		15: "PC15 - HMI communication Fault",
	}
)

// EquipConfig définit la configuration pour chaque équipement
type EquipConfig struct {
	ICField string
	PCField string
	ICMap   map[int]string
	PCMap   map[int]string
	Title   string
	EqpName string
}

var equipConfigs = map[string]EquipConfig{
	"AC": {
		ICField: "SEQ01.OLI.A.IC1",
		PCField: "SEQ01.OLI.A.PC1",
		ICMap:   IC1_SEQ01_MAP,
		PCMap:   PC1_SEQ01_MAP,
		Title:   "AC (SEQ01)",
		EqpName: "Variateur AFE",
	},
	"DC1": {
		ICField: "SEQ02.OLI.A.IC1",
		PCField: "SEQ02.OLI.A.PC1",
		ICMap:   IC1_SEQ02_MAP,
		PCMap:   PC1_SEQ02_MAP,
		Title:   "Batterie DC1 (SEQ02)",
		EqpName: "Variateur HB1",
	},
	"DC2": {
		ICField: "SEQ03.OLI.A.IC1",
		PCField: "SEQ03.OLI.A.PC1",
		ICMap:   IC1_SEQ03_MAP,
		PCMap:   PC1_SEQ03_MAP,
		Title:   "Batterie DC2 (SEQ03)",
		EqpName: "Variateur HB2",
	},
	"PDC1": {
		ICField: "SEQ12.OLI.A.IC1",
		PCField: "SEQ12.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 1 (SEQ12)",
		EqpName: "HC1 (PDC1)",
	},
	"PDC2": {
		ICField: "SEQ22.OLI.A.IC1",
		PCField: "SEQ22.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 2 (SEQ22)",
		EqpName: "HC1 (PDC2)",
	},
	"PDC3": {
		ICField: "SEQ13.OLI.A.IC1",
		PCField: "SEQ13.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 3 (SEQ13)",
		EqpName: "HC2 (PDC3)",
	},
	"PDC4": {
		ICField: "SEQ23.OLI.A.IC1",
		PCField: "SEQ23.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 4 (SEQ23)",
		EqpName: "HC2 (PDC4)",
	},
	"PDC5": {
		ICField: "SEQ14.OLI.A.IC1",
		PCField: "SEQ14.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 5 (SEQ14)",
		EqpName: "HC3 (PDC5)",
	},
	"PDC6": {
		ICField: "SEQ24.OLI.A.IC1",
		PCField: "SEQ24.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 6 (SEQ24)",
		EqpName: "HC3 (PDC6)",
	},
}

// DefautRecord représente un enregistrement de défaut
type DefautRecord struct {
	Site   string
	Date   time.Time
	Defaut string
	Eqp    string
}

// LastValues stocke les dernières valeurs pour détecter les changements
type LastValues struct {
	values map[string]map[string]float64 // [site][field]value
}

func NewLastValues() *LastValues {
	return &LastValues{
		values: make(map[string]map[string]float64),
	}
}

func (lv *LastValues) Get(site, field string) (float64, bool) {
	if siteMap, ok := lv.values[site]; ok {
		val, exists := siteMap[field]
		return val, exists
	}
	return 0, false
}

func (lv *LastValues) Set(site, field string, value float64) {
	if _, ok := lv.values[site]; !ok {
		lv.values[site] = make(map[string]float64)
	}
	lv.values[site][field] = value
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// decodeBinary décode un nombre en binaire et retourne les positions des bits à 1
func decodeBinary(value int) []int {
	var positions []int
	for i := 0; i < 32; i++ {
		if value&(1<<i) != 0 {
			positions = append(positions, i)
		}
	}
	return positions
}

// insertDefaut insère un défaut dans la base de données
func insertDefaut(db *sql.DB, record DefautRecord) error {
	query := `INSERT INTO kpi_defauts (site, date, defaut, eqp) VALUES (?, ?, ?, ?)`
	_, err := db.Exec(query, record.Site, record.Date, record.Defaut, record.Eqp)
	if err != nil {
		return fmt.Errorf("erreur lors de l'insertion: %w", err)
	}
	return nil
}

// processFieldValue traite une valeur de champ et insère les défauts si nécessaire
func processFieldValue(db *sql.DB, site string, field string, value float64, fieldMap map[int]string, eqpName string, lastValues *LastValues) error {
	// Vérifier si la valeur a changé
	lastVal, exists := lastValues.Get(site, field)
	if exists && lastVal == value {
		return nil // Aucun changement
	}

	// Mettre à jour la dernière valeur
	lastValues.Set(site, field, value)

	intValue := int(value)
	if intValue == 0 {
		// Pas de défaut
		return nil
	}

	// Décoder en binaire
	activeBits := decodeBinary(intValue)

	// Créer un enregistrement pour chaque bit actif
	now := time.Now()
	for _, bitPos := range activeBits {
		if defautDesc, ok := fieldMap[bitPos]; ok && defautDesc != "" {
			record := DefautRecord{
				Site:   site,
				Date:   now,
				Defaut: defautDesc,
				Eqp:    eqpName,
			}

			err := insertDefaut(db, record)
			if err != nil {
				log.Printf("Erreur lors de l'insertion du défaut: %v", err)
			} else {
				log.Printf("Défaut détecté - Site: %s, Eqp: %s, Défaut: %s", site, eqpName, defautDesc)
			}
		}
	}

	return nil
}

// queryInflux interroge InfluxDB pour tous les sites et champs configurés
func queryInflux(queryAPI api.QueryAPI, db *sql.DB, lastValues *LastValues) error {
	// Construire la liste des champs à surveiller
	var fields []string
	for _, config := range equipConfigs {
		fields = append(fields, config.ICField, config.PCField)
	}

	// Créer une requête InfluxDB pour récupérer les dernières valeurs
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -1m)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => %s)
			|> last()
	`, influxDB, influxMeas, buildFieldFilter(fields))

	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("erreur lors de la requête InfluxDB: %w", err)
	}

	// Traiter les résultats
	for result.Next() {
		record := result.Record()
		site := record.ValueByKey(influxTagProject)
		if site == nil {
			continue
		}
		siteStr := fmt.Sprintf("%v", site)

		field := record.Field()
		value := record.Value()

		// Convertir la valeur en float64
		var floatValue float64
		switch v := value.(type) {
		case float64:
			floatValue = v
		case int64:
			floatValue = float64(v)
		case int:
			floatValue = float64(v)
		default:
			continue
		}

		// Trouver la configuration correspondante et traiter la valeur
		for _, config := range equipConfigs {
			if field == config.ICField {
				processFieldValue(db, siteStr, field, floatValue, config.ICMap, config.EqpName, lastValues)
			} else if field == config.PCField {
				processFieldValue(db, siteStr, field, floatValue, config.PCMap, config.EqpName, lastValues)
			}
		}
	}

	if result.Err() != nil {
		return fmt.Errorf("erreur lors de la lecture des résultats: %w", result.Err())
	}

	return nil
}

// buildFieldFilter construit un filtre pour les champs InfluxDB
func buildFieldFilter(fields []string) string {
	var filters []string
	for _, field := range fields {
		filters = append(filters, fmt.Sprintf(`r["_field"] == "%s"`, field))
	}
	return strings.Join(filters, " or ")
}

func main() {
	log.Println("Démarrage du watcher InfluxDB...")

	// Connexion à MySQL
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("Erreur de connexion à MySQL: %v", err)
	}
	defer db.Close()

	// Tester la connexion MySQL
	err = db.Ping()
	if err != nil {
		log.Fatalf("Impossible de se connecter à MySQL: %v", err)
	}
	log.Println("Connexion à MySQL établie")

	// Connexion à InfluxDB
	influxURL := fmt.Sprintf("https://%s:%s", influxHost, influxPort)
	client := influxdb2.NewClientWithOptions(
		influxURL,
		"",
		influxdb2.DefaultOptions().
			SetHTTPRequestTimeout(30).
			SetTLSConfig(&tls.Config{InsecureSkipVerify: true}),
	)
	defer client.Close()

	// Créer l'API de requête avec authentification basique
	queryAPI := client.QueryAPI("")

	log.Printf("Connexion à InfluxDB: %s", influxURL)
	log.Printf("Base de données: %s, Measurement: %s", influxDB, influxMeas)

	// Initialiser le stockage des dernières valeurs
	lastValues := NewLastValues()

	// Boucle principale de surveillance
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	defer ticker.Stop()

	log.Printf("Surveillance active (intervalle: %d secondes)", pollInterval)

	// Première exécution immédiate
	if err := queryInflux(queryAPI, db, lastValues); err != nil {
		log.Printf("Erreur lors de la première requête: %v", err)
	}

	// Boucle de surveillance
	for range ticker.C {
		if err := queryInflux(queryAPI, db, lastValues); err != nil {
			log.Printf("Erreur lors de la surveillance: %v", err)
		}
	}
}
