package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts
	MAQUINA1 = "127.0.0.1"
	MAQUINA2 = "127.0.0.1"
	MAQUINA3 = "127.0.0.1"

	//puertos
	PUERTOREPLICA1 = "29041"
	PUERTOREPLICA2 = "29042"
	PUERTOREPLICA3 = "29043"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_ed25519"
)

// PATH de los ejecutables de modulo golang de servicio Raft
var PATH string = filepath.Join(os.Getenv("HOME"), "CodigoEsqueleto", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; /usr/local/go/bin/go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 0, false, -1)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	lider := cfg.pruebaUnLider(3)
	fmt.Println("El lider es", lider)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	lider := cfg.pruebaUnLider(3)

	// Desconectar lider
	fmt.Printf("Desconectar lider: %d\n", lider)
	cfg.desconectarLider(lider)

	fmt.Printf("Comprobar nuevo lider\n")
	segLider := cfg.pruebaUnLider(3)
	fmt.Printf("Nuevo lider: %d\n", segLider)

	cfg.startDistributedProcess(lider)

	time.Sleep(1 * time.Second)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	lider := cfg.pruebaUnLider(3)
	fmt.Println("El lider es", lider)
	var indice int
	for i := 0; i < 3; i++ {
		indice = cfg.comprometerUnaEntrada(lider, "leer", strconv.Itoa(i), "")
		time.Sleep(1 * time.Second)
	}

	if cfg.comprobarEntradaComprometida(lider, indice) {
		fmt.Printf("Correcto: Hay %d entradas comprometidas\n", indice)
	} else {
		fmt.Printf("Entrada no comprometida\n")
	}
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	// A completar ???
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Comprometer una entrada
	//time.Sleep(10 * time.Second)
	fmt.Printf("Comprometiendo una entrada: 0, leer, nueva\n")
	indice := cfg.comprometerUnaEntrada(0, "leer", "nueva", "")
	time.Sleep(5 * time.Second)
	if cfg.comprobarEntradaComprometida(2, indice) {
		fmt.Printf("Entrada comprometida correctamente\n")
	} else {
		fmt.Printf("Entrada no comprometida\n")
	}

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft
	lider := cfg.pruebaUnLider(3)
	fmt.Printf("Lider inicial: %d\n", lider)
	seguidor := cfg.desconectarSeguidor(lider, 3)

	// Comprobar varios acuerdos con una réplica desconectada
	for i := 0; i < 3; i++ {
		fmt.Printf("Comprometiendo una entrada: 0, leer, nueva\n")
		indice = cfg.comprometerUnaEntrada(lider, "escribir", "nueva", strconv.Itoa(i))
		time.Sleep(3 * time.Second)
		if cfg.comprobarEntradaComprometida(lider, indice) {
			fmt.Printf("Entrada comprometida correctamente\n")
		}
	}

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
	cfg.startDistributedProcess(seguidor)
	time.Sleep(10 * time.Second)
	//seguidor = 2
	fmt.Printf("Vamos a comprobar las entradas comprometidas del nodo: %d\n", seguidor)
	if cfg.comprobarEntradaComprometida(seguidor, indice) {
		fmt.Printf("Correcto: Hay %d entradas comprometidas\n", indice)
	} else {
		fmt.Printf("Entrada no comprometida\n")
	}

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")

}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	//t.Skip("SKIPPED SinAcuerdoPorFallos")

	// A completar ???
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Comprometer una entrada
	cfg.comprometerUnaEntrada(0, "escribir", "nueva", "afjsl")

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	lider := cfg.pruebaUnLider(3)
	fmt.Printf("Lider inicial: %d\n", lider)
	seguidor := cfg.desconectarSeguidor(lider, 3)
	seguidor2 := cfg.desconectarSeguidor(lider, 3)

	// Comprobar varios acuerdos con 2 réplicas desconectada
	time.Sleep(3 * time.Second)
	i := 0
	for i = 0; i < 3; i++ {
		indice := cfg.comprometerUnaEntrada(lider, "escribir", "nueva", strconv.Itoa(i))
		if cfg.comprobarEntradaComprometida(lider, indice) {
			fmt.Printf("Correcto: Hay %d entradas comprometidas\n", indice)
		} else {
			fmt.Printf("Falso: No hay %d entradas comprometidas\n", indice)
		}
	}

	// reconectar lo2 nodos Raft  desconectados y probar varios acuerdos
	fmt.Printf("Reconectando nodo: %d\n", seguidor)
	cfg.startDistributedProcess(seguidor)
	fmt.Printf("Reconectando nodo: %d\n", seguidor2)
	cfg.startDistributedProcess(seguidor2)
	//time.Sleep(30 * time.Second)
	if cfg.comprobarEntradaComprometida(seguidor, i+1) {
		fmt.Printf("Correcto: Hay %d entradas comprometidas\n", i+1)
	} else {
		fmt.Printf("Falso: No hay %d entradas comprometidas\n", i+1)
	}
	if cfg.comprobarEntradaComprometida(seguidor2, i+1) {
		fmt.Printf("Correcto: Hay %d entradas comprometidas\n", i+1)
	} else {
		fmt.Printf("Falso: No hay %d entradas comprometidas\n", i+1)
	}

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	// A completar ???
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// un bucle para estabilizar la ejecucion
	time.Sleep(10 * time.Second)

	// Obtener un lider y, a continuación someter una operacion
	lider := cfg.pruebaUnLider(3)
	cfg.comprometerUnaEntrada(lider, "escribir", "nueva", strconv.Itoa(0))

	// Someter 5  operaciones concurrentes
	for i := 0; i < 5; i++ {
		go cfg.comprometerUnaEntrada(lider, "escribir", "nueva", strconv.Itoa(i+1))
	}

	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
	time.Sleep(15 * time.Second)
	for i := range cfg.nodosRaft {
		fmt.Printf("Nodo: %d\n", i)
		if cfg.comprobarEntradaComprometida(i, 6) {
			fmt.Printf("Correcto: Hay %d entradas comprometidas\n", 6)
		} else {
			fmt.Printf("Entrada no comprometida\n")
		}
	}

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

//Nuestras funciones
func (cfg *configDespliegue) desconectarSeguidor(lider int, numNodos int) int {
	//rand.Seed(int64(lider + numNodos))
	//matar := rand.Intn(numNodos)
	matar := 0
	for i := 0; i < numNodos; i++ {
		if (i != lider) && cfg.conectados[i] {
			matar = i
		}
	}
	/*for !cfg.conectados[matar] {
		//fmt.Printf("Vamos ha desconectar nodo: %d\n", matar)
		if matar == lider {
			if lider == 0 {
				matar++
			} else {
				matar--
			}
		} else {
			matar = rand.Intn(numNodos)
		}
	}*/
	var reply raft.Vacio

	fmt.Printf("Desconectando seguidor: %d\n", matar)
	err := cfg.nodosRaft[matar].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 100*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")

	cfg.conectados[matar] = false

	return matar
}

func (cfg *configDespliegue) comprometerUnaEntrada(lider int, operacion string, clave string, valor string) int {
	var reply raft.ResultadoRemoto
	args := raft.TipoOperacion{Operacion: operacion, Clave: clave, Valor: valor}
	//No sabemos quien es el lider, preguntamos a 0
	err := cfg.nodosRaft[lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		args, &reply, 2*time.Second)
	//check.CheckError(err, "Error en llamada RPC Para nodo")
	for {
		switch {
		case err != nil:
			fmt.Println("Error comprometer una entrada")
			return -1
		case !reply.EsLider:
			fmt.Println("Redirigiendo comprometer una entrada a nodo", reply.IdLider)
			err = cfg.nodosRaft[reply.IdLider].CallTimeout("NodoRaft.SometerOperacionRaft",
				args, &reply, 1*time.Second)
			//check.CheckError(err, "Error en llamada RPC Para nodo")
			lider = reply.IdLider
		case reply.EsLider:
			fmt.Printf("El nodo %d ha sometido la operacion\n", lider)
			return reply.IndiceRegistro
		}
	}
}

func (cfg *configDespliegue) comprobarEntradaComprometida(nodo int, indice int) bool {
	var reply int
	time.Sleep(1500 * time.Millisecond)
	err := cfg.nodosRaft[nodo].CallTimeout("NodoRaft.Comited",
		raft.Vacio{}, &reply, 1000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC comprobarEntradaComprometida")
	fmt.Printf("Indice: %d, Reply: %d", indice, reply)
	return indice == reply
}

func (cfg *configDespliegue) desconectarLider(lider int) {
	var reply raft.Vacio

	err := cfg.nodosRaft[lider].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 100*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")

	cfg.conectados[lider] = false
	//time.Sleep(15 * time.Second)
}

func (cfg *configDespliegue) startDistributedProcess(nodo int) {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	despliegue.ExecMutipleHosts(EXECREPLICACMD+
		" "+strconv.Itoa(nodo)+" "+
		rpctimeout.HostPortArrayToString(cfg.nodosRaft),
		[]string{cfg.nodosRaft[nodo].Host()}, cfg.cr, PRIVKEYFILE)

	// dar tiempo para se establezcan las replicas
	//time.Sleep(500 * time.Millisecond)
	cfg.conectados[nodo] = true

	time.Sleep(1 * time.Second)
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(1500 * time.Millisecond) //antes eran 500 ms
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

		// dar tiempo para se establezcan las replicas
		//time.Sleep(500 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2500 * time.Millisecond)
}

//
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for _, endPoint := range cfg.nodosRaft {
		err := endPoint.CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &reply, 10*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	//cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}

}
