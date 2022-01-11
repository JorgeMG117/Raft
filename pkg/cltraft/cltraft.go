package cltraft

import (
	"fmt"
	"raft/internal/comun/rpctimeout"
	"time"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func Client(endPoint string) {
	// obtener entero de indice de este nodo
	/*
		me, err := strconv.Atoi(os.Args[1])
		check.CheckError(err, "Main, mal numero entero de indice de nodo:")

		var nodos []rpctimeout.HostPort
		// Resto de argumento son los end points como strings
		// De todas la replicas-> pasarlos a HostPort
		for _, endPoint := range os.Args[2:] {
			nodos = append(nodos, rpctimeout.HostPort(endPoint))
		}

		// Parte Servidor
		nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
		rpc.Register(nr)

		//fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])

		l, err := net.Listen("tcp", os.Args[2:][me])
		check.CheckError(err, "Main listen error:")

		rpc.Accept(l)*/

	for {
		time.Sleep(time.Second * 15)
		fmt.Println("Empezando cliente,", endPoint)

		/*client, err := rpc.DialHTTP("tcp", endPoint)
		if err != nil {
			log.Fatal("dialing:", err)
		}*/

		operacion := TipoOperacion{"escribir", "b", ""}
		var reply ResultadoRemoto
		//err = client.Call("raft.SometerOperacionRaft", operacion, &reply)
		err := rpctimeout.HostPort.CallTimeout(rpctimeout.HostPort(endPoint), "NodoRaft.SometerOperacionRaft", operacion, &reply, 5*time.Second)
		if err != nil {
			fmt.Println("SometerOperacionRaft error:", err)
		}
		fmt.Println("Reply", endPoint, ": ValorADevolver:", reply.ValorADevolver, "IndiceRegistro:", reply.IndiceRegistro, "Mandato:", reply.Mandato,
			"EsLider", reply.EsLider, "IdLider", reply.IdLider)

	}
}
