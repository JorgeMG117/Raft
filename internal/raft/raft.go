// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	commitIndex int
	lastApplied int

	// Vuestros datos aqui.
	log         []AplicaOperacion
	currentTerm int
	votedFor    int
	logIndex    int

	nextIndex  []int
	matchIndex []int

	chReinicioTimeout chan bool
	chGotVote         chan bool
	//ch chan
	electionTimeout time.Duration

	maquinaDeEstados map[string]string

	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	// Añadir codigo de inicialización

	//nr.electionTimeout = time.Millisecond*10000*(time.Duration(nr.Yo)+1) + time.Millisecond*time.Duration(nr.Yo)*10000
	rand.Seed(int64(nr.Yo))
	nr.electionTimeout = time.Millisecond * time.Duration(rand.Intn(500)+1000)
	fmt.Println(nr.Yo, "Election timeout:", nr.electionTimeout)
	nr.currentTerm = 0
	nr.votedFor = -1

	nr.lastApplied = 0
	nr.commitIndex = 0

	nr.chReinicioTimeout = make(chan bool)
	nr.chGotVote = make(chan bool)
	nr.log = make([]AplicaOperacion, 10)
	//nr.log = []AplicaOperacion{}
	nr.nextIndex = make([]int, len(nr.Nodos))
	nr.matchIndex = make([]int, len(nr.Nodos))

	nr.logIndex = 0
	//nr.Mux.Unlock()

	go gestionNodo(nr)
	go actualizarMaquinaEstados(nr)

	return nr
}

func gestionNodo(nr *NodoRaft) {
	//time.Sleep(1 * time.Second)
	for {
		_, _, esLider, _ := nr.obtenerEstado()
		switch esLider {
		case true:
			leader(nr)
		case false:
			follower(nr)
		default:
			fmt.Println(nr.Yo, ". Tienes issues")
			os.Exit(1)
		}
	}
}

func follower(nr *NodoRaft) {
	//fmt.Println(nr.Yo, ". entrando a funcion follower")
	select {
	case <-nr.chReinicioTimeout: //Nos llega un rpc
		//fmt.Println(nr.Yo, ". Me ha llegado un rpc")
	case <-time.After(nr.electionTimeout):
		//fmt.Println(nr.Yo, " yo voy a ser candidato")
		//Pasas a candidato
		candidate(nr)
	}
}

func candidate(nr *NodoRaft) {
	fmt.Println(nr.Yo, ". SOY CANDIDATO")
	//Increment current term
	nr.currentTerm++
	nr.votedFor = -1

	//Vote itself
	nr.votedFor = nr.Yo
	numVotos := 1
	fmt.Println(nr.Yo, "Me voto a mi mismo")

	//Reset election timeout
	args := ArgsPeticionVoto{nr.Yo, nr.currentTerm, nr.logIndex, nr.log[nr.logIndex].Indice}

	for i := range nr.Nodos {
		if i != nr.Yo {
			var reply RespuestaPeticionVoto
			go nr.enviarPeticionVoto(i, &args, &reply)
		}

	}

	out := false
	for (nr.IdLider != nr.Yo) && !out {
		select {
		case <-nr.chGotVote:
			//fmt.Println(nr.Yo, ". Me ha llegado un voto")
			numVotos++
			if numVotos > (len(nr.Nodos) / 2) {
				nr.IdLider = nr.Yo
				fmt.Println(nr.Yo, "Mayoria de votos conseguidos")
			}
		case <-nr.chReinicioTimeout:
			//fmt.Println(nr.Yo, ". Me ha llegado chReinicioTimeout")
			out = true
			nr.electionTimeout = nr.electionTimeout + time.Duration(nr.Yo*20000000)
		case <-time.After(1100 * time.Millisecond): //rand.Intn(20-2) + 2
			fmt.Println(nr.Yo, ". No se ha resuelto la eleccion, vamos a reiniciarla")
			candidate(nr)
			out = true
		}
	}
}

func (nr *NodoRaft) enviarAppendEntries(nodo int, args *ArgAppendEntries, reply *Results) bool {

	timeout := time.Duration(500 * time.Millisecond)
	err := rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.AppendEntries", args, reply, timeout)
	exit := false

	go func() {
		for !exit {
			switch {
			/*case nr.currentTerm > args.Term || nr.commitIndex > args.LeaderCommit:
			exit = true*/
			case reply.Term > nr.currentTerm:
				fmt.Println("Voy retrasado, mi term,", nr.currentTerm, "term de", nodo, ":", reply.Term, "en enviarAppendEntries")
				nr.IdLider = -1
				nr.chReinicioTimeout <- true
				exit = true
				//return false
			case err != nil:
				fmt.Println(nr.Yo, ". Error Append Entries", nodo, " ", err.Error())
				//fmt.Fprintf(os.Stderr, "In: %s, Fatal error: %s", comment, err.Error())
				time.Sleep(200 * time.Millisecond)
				err = rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.AppendEntries", args, reply, timeout)

			case !(reply.Success) && (len(args.Entries) > 0):

				nr.nextIndex[nodo]--
				fmt.Println(nr.Yo, ". No se ha conseguido replicar entrada a", nodo, "nr.nextIndex[nodo]: ", nr.nextIndex[nodo])
				//entries := nr.log[(nr.nextIndex[nodo]):(nr.commitIndex + 1)]
				entries := nr.log[(nr.nextIndex[nodo]):(nr.logIndex + 1)]
				args := ArgAppendEntries{nr.currentTerm, nr.Yo, nr.nextIndex[nodo] - 1, nr.log[nr.nextIndex[nodo]-1].Indice, entries, nr.commitIndex, args.chCommit}
				rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.AppendEntries", args, reply, timeout)

			case reply.Success && (len(args.Entries) > 0):

				//Update nextIndex and matchIndex
				//nr.nextIndex[nodo] = nr.commitIndex + 1
				nr.nextIndex[nodo] = nr.logIndex + 1
				nr.matchIndex[nodo] = nr.logIndex
				fmt.Println(nr.Yo, ". Se ha conseguido replicar entrada a", nodo, "nr.nextIndex[nodo]: ", nr.nextIndex[nodo])
				exit = true
				args.chCommit <- true
			default:
				//fmt.Println(nr.Yo, ". Default enviarAppendEntries")
				//os.Exit(1)
				exit = true
			}

		}

	}()

	//<-time.After(nr.electionTimeout)
	//exit = true
	return exit

}

func leader(nr *NodoRaft) {
	fmt.Println("SOY LIDER ", nr.Yo, "Term:", nr.currentTerm, "Lider:", nr.IdLider)

	//Enviar latidos
	//mirar que seas lider, si te responden que no eres lider dedjas de serlo(comparar terms)
	go func() {
		fmt.Println("enviando latidos")
		for nr.IdLider == nr.Yo {
			var entries []AplicaOperacion
			for i := range nr.Nodos {
				if i != nr.Yo {
					args := ArgAppendEntries{nr.currentTerm, nr.Yo, 0, 0, entries, nr.commitIndex, make(chan bool, 1)}
					reply := Results{}
					//fmt.Println("enviando latidos")
					go nr.enviarAppendEntries(i, &args, &reply)
				}
			}
			time.Sleep(50 * time.Millisecond)

		}
	}()

	for i := range nr.Nodos {
		nr.nextIndex[i] = nr.logIndex + 1
		nr.matchIndex[i] = 0
	}

	go actualizarCommitIndex(nr)

	<-nr.chReinicioTimeout
	fmt.Println(nr.Yo, "Dejo de ser LIDER, Yo:", nr.Yo, "Lider:", nr.IdLider)
	//Decirte a ti mismo que ya no eres lider??
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.currentTerm
	var esLider bool = (nr.IdLider == nr.Yo)
	var idLider int = nr.IdLider

	// Vuestro codigo aqui

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	//indice := -1
	//mandato := -1
	//EsLider := false
	//idLider := -1
	valorADevolver := ""

	// Si el nodo no es el lider, devolver falso
	if nr.IdLider != nr.Yo {
		return -1, nr.currentTerm, false, nr.IdLider, valorADevolver
	}

	//si eres lider
	//Meter nosostros en el log
	/*
		nr.commitIndex = nr.commitIndex + 1
		nr.log[nr.commitIndex].Operacion = operacion
		nr.log[nr.commitIndex].Indice = nr.currentTerm*/

	nr.logIndex++
	indiceAux := nr.logIndex
	nr.log[nr.logIndex].Operacion = operacion
	nr.log[nr.logIndex].Indice = nr.currentTerm
	nr.matchIndex[nr.Yo] = nr.logIndex

	chCommit := make(chan bool, 1)

	for i := range nr.Nodos {
		if i != nr.Yo {
			entries := nr.log[(nr.nextIndex[i]):(nr.logIndex + 1)]
			//fmt.Println("nextIndex: ", nr.nextIndex[i])
			//fmt.Println("nextIndexYO: ", nr.nextIndex[nr.Yo])
			fmt.Println("Vamos a enviar:", entries)
			args := ArgAppendEntries{nr.currentTerm, nr.Yo, nr.nextIndex[i] - 1, nr.log[nr.nextIndex[i]-1].Indice, entries, nr.commitIndex, chCommit}
			reply := Results{}
			fmt.Println("enviando op")
			go nr.enviarAppendEntries(i, &args, &reply)
		}
	}

	for i := 0; i < (len(nr.Nodos)-1)/2; i++ {
		<-chCommit
	}

	/*if nr.log[indiceAux].Indice == nr.currentTerm {
		fmt.Println(nr.Yo, ". Ya es seguro hacer commit, commitIndex =", indiceAux)
		nr.commitIndex = indiceAux
	}*/

	//fmt.Println("someter operacion ", nr.Yo)
	return indiceAux, nr.currentTerm, true, nr.IdLider, valorADevolver //Ojo con lo que devuelve nr.commitIndex
}

/*if there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm:
set commitIndex = N */

func actualizarMaquinaEstados(nr *NodoRaft) {
	for {
		time.Sleep(100 * time.Millisecond)
		if nr.commitIndex > nr.lastApplied {
			nr.lastApplied++

			if nr.log[nr.lastApplied].Operacion.Operacion == "leer" {
				_ = nr.maquinaDeEstados[nr.log[nr.lastApplied].Operacion.Clave]
			} else if nr.log[nr.lastApplied].Operacion.Operacion == "escribir" {
				nr.maquinaDeEstados[nr.log[nr.lastApplied].Operacion.Clave] = nr.log[nr.lastApplied].Operacion.Valor
			} else {
				fmt.Println("Operacion no reconocida")
			}
		}
	}
}

func actualizarCommitIndex(nr *NodoRaft) {
	for nr.IdLider == nr.Yo {
		nuevoCommitIndex := 0
		for _, i := range nr.matchIndex {
			estaReplicado := 0
			if i > nr.commitIndex && i > nuevoCommitIndex && nr.log[i].Indice == nr.currentTerm {
				//fmt.Println("vamos a ver si ", i, "es el nuevo commit index")
				for _, j := range nr.matchIndex {
					if i <= j {
						estaReplicado++
					}
				}
				if estaReplicado > (len(nr.Nodos)-1)/2 {
					nuevoCommitIndex = i
				}
			}
		}
		if nuevoCommitIndex > 0 {
			nr.commitIndex = nuevoCommitIndex
			fmt.Println(nr.Yo, ". Nuevo commit index:", nr.commitIndex)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

func (nr *NodoRaft) Comited(args Vacio, reply *int) error {
	nr.comited(reply)
	return nil
}

func (nr *NodoRaft) comited(reply *int) {
	*reply = nr.commitIndex
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	//fmt.Println("Mi estado: ", reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider)
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	Term        int
	VoteGranted bool
}

// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {

	fmt.Println(nr.Yo, ". Me han pedido que vote a", peticion.CandidateId)
	reply.VoteGranted = false
	// Vuestro codigo aqui
	if peticion.Term < nr.currentTerm {
		fmt.Println("el candidato es malo", peticion.CandidateId)

		reply.Term = nr.currentTerm
		// el que llama a este rpc se vuelve follower
	} else if peticion.Term >= nr.currentTerm {
		if peticion.Term > nr.currentTerm {
			fmt.Println(nr.Yo, ". Voy retrasado, mi term", nr.currentTerm, "term de peticion", peticion.Term)
			//Reiniciar votedFor
			nr.votedFor = -1
			nr.currentTerm = peticion.Term

			nr.IdLider = -1
		}

		if nr.IdLider != nr.Yo {
			if nr.votedFor == -1 &&
				(peticion.LastLogTerm > nr.log[nr.logIndex].Indice || (peticion.LastLogTerm == nr.log[nr.logIndex].Indice && peticion.LastLogIndex >= nr.logIndex)) {

				fmt.Println(nr.Yo, ". he votado a", peticion.CandidateId)
				nr.votedFor = peticion.CandidateId
				reply.VoteGranted = true
			}
			nr.chReinicioTimeout <- true
		}

	}
	return nil
}

type ArgAppendEntries struct {
	// Vuestros datos aqui
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []AplicaOperacion

	LeaderCommit int

	chCommit chan bool
}

type Results struct {
	// Vuestros datos aqui
	Term    int
	Success bool
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	// Completar....
	//fmt.Println(nr.Yo, ". Me ha llegado AppendEntries de", args.LeaderId)
	aux := nr.logIndex
	switch {
	case args.Term < nr.currentTerm:
		//fmt.Println(nr.Yo, ". AppendEntries -> args.Term < nr.currentTerm")
		results.Term = nr.currentTerm
		//results.Success = false
	case args.Term >= nr.currentTerm:
		//fmt.Println(nr.Yo, ". AppendEntries -> args.Term >= nr.currentTerm")
		if args.Term > nr.currentTerm {
			//Reiniciar votedFor
			nr.votedFor = -1
			nr.currentTerm = args.Term
		}

		nr.IdLider = args.LeaderId
		//results.Success = true

		if len(args.Entries) > 0 {
			switch {
			case nr.log[args.PrevLogIndex].Indice != args.PrevLogTerm: //prevlogindex igual no
				//fmt.Println(nr.Yo, ".  nr.log[args.PrevLogIndex].Indice != args.PrevLogTerm")
				results.Success = false
			case nr.log[args.PrevLogIndex].Indice == args.PrevLogTerm:
				//fmt.Println(nr.Yo, ".  nr.log[args.PrevLogIndex].Indice == args.PrevLogTerm")
				//copiar entries desde prevlogindex, que igual no es es prevlogindex
				i := 0
				for ; i < len(args.Entries); i++ {
					nr.log[args.PrevLogIndex+1+i] = args.Entries[i]
				}

				//nr.Mux.Lock()
				nr.logIndex = args.PrevLogIndex + i
				aux = nr.logIndex
				//nr.Mux.Unlock()

				results.Success = true
				fmt.Println(nr.Yo, ". He hecho commit en log: ", nr.log)
			}
		}

		nr.chReinicioTimeout <- true

	}

	if args.LeaderCommit > nr.commitIndex {

		if args.LeaderCommit < aux {
			nr.commitIndex = args.LeaderCommit
		} else {
			nr.commitIndex = aux
		}
		/*fmt.Println(nr.Yo, "LeaderCommit: ", args.LeaderCommit)
		fmt.Println(nr.Yo, "CommitIndex: ", nr.commitIndex)
		fmt.Println(nr.Yo, "LogIndex: ", aux)*/
	}

	return nil

}

// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	// Completar....
	timeout := time.Duration(500 * time.Millisecond)
	//fmt.Println(nr.Yo, ". voy a pedir votos a ", nodo)

	err := rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.PedirVoto", args, reply, timeout)
	//fmt.Println(nr.Yo, "he completado rpc")
	//check.CheckError(err, "Error Pedir Voto")
	for {
		switch {
		case nr.IdLider == nr.Yo:
			fmt.Println(nr.Yo, " .Ya soy lider, saliendo de enviarPeticionVoto")
			return true
		case args.Term != nr.currentTerm: //Ya no eres candidato, estamos en otro term
			fmt.Println(nr.Yo, " .No soy candidato estamos en otro term")
			return true
		case err != nil:
			//fmt.Println(nr.Yo, ". otro try candidato, Error Pedir Voto a ", nodo)
			//Habria que deja tiempo para que no spameara????
			time.Sleep(1 * time.Second)
			err = rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.PedirVoto", args, reply, timeout)

		case reply.Term > nr.currentTerm:
			fmt.Println(nr.Yo, ". No soy candidato voy de lado") //Habria que cambiar term????
			nr.currentTerm = reply.Term
			nr.chReinicioTimeout <- true
			return true
		case reply.VoteGranted:
			if nr.IdLider != nr.Yo { //Mutex
				fmt.Println(nr.Yo, ". Me ha votado", nodo)
				nr.chGotVote <- true
				return true
			}
			return false
		default:
			fmt.Println(nr.Yo, ". Default enviarPeticionVoto")
			//os.Exit(1)
			return false
		}
	}
}
