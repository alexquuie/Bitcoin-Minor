import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.routing.RoundRobinRouter
import akka.routing.ActorRefRoutee
import akka.routing.RoundRobinRoutingLogic
import akka.routing.Router
import akka.routing.RoundRobinPool

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.net.InetAddress
import java.security.MessageDigest
import java.util.ArrayList
import java.util.List
import java.util.regex.Pattern

case object Start
case class ClientAvailiableNow(clientInfo : String)
case class SendCoinsToServer(coins : List[String])
case class AssignWork(k : Int, begin : Long, workSize : Int)
case class CoinsFoundByWoker(coins : List[String])
case class Mining(k : Int, block : Long, blockSize : Int)
case class Stop()
case object Stop

object Project {
	def main(args : Array[String]) {
		val (ip, k, numOfCoins, roleInSystem) = readArgs(args)
		val appConfig = ConfigFactory.load()
		if (roleInSystem == "Server") {
			val hostIP = InetAddress.getLocalHost().getHostAddress()
			val serverConfigStr = """
					akka {
  						actor {
    						provider = "akka.remote.RemoteActorRefProvider"
  						}
  						remote {
    						netty.tcp {
      						hostname = """" + hostIP + """"
      						port = 5001
    						}
  						}
					}"""
			val serverAppConfig = appConfig.getConfig("LocalSystem")
			val serverConfig = ConfigFactory.parseString(serverConfigStr)
			val serverSystem = ActorSystem("ServerConfiguration",ConfigFactory.load(serverConfig).withFallback(serverAppConfig))
			val server = serverSystem.actorOf(Props(classOf[Server], k , numOfCoins, serverAppConfig), name = "server")			
		}

		if (roleInSystem == "Client") {
			val serverPath = "akka.tcp://" + "ServerConfiguration" + "@" + ip + ":5001/user/server"
			println(serverPath)
			val clienConfig = appConfig.getConfig("RemoteSystem")
			
      		val clientSystem = ActorSystem("ClientConfiguration", clienConfig)
		    val client = clientSystem.actorOf(Props(classOf[Client], serverPath,  clienConfig), name = "RemoteCLient")
    
		}
	}

	def readArgs(args : Array[String]) : (String, Int, Int, String) = {
		var ip : String = ""
		var roleInSystem : String = ""
		var k : Int = -1
		var numOfCoins : Int = -1
		if(args(0).contains('.')) {
			ip = args(0)
			roleInSystem = "Client"
		} else {
			k = args(0).toInt
			if(args.length == 2) {
			numOfCoins = args(1).toInt
			}
			roleInSystem = "Server"
		}
		return (ip, k, numOfCoins, roleInSystem)
	}


  class Server(k: Int, numOfCoins: Int, config: Config) extends Actor {
  	val localClient = context.actorOf(Props(classOf[Client],"",config), name = "LocalClient")
  	localClient ! Start
	var begin: Long = 0
    val workSize: Int = config.getInt("WorkSize")
    var totalFound = 0;

  	def receive = {
  		case ClientAvailiableNow(clientInfo) => {
  			println(clientInfo)
  			sender ! AssignWork(k, begin, workSize)
  			begin = begin + workSize
  		}
  		case SendCoinsToServer(coins) => {
  			totalFound = totalFound + coins.size()
  			for(i <- 0 to coins.size() - 1){
  				println(coins.get(i))
  			}
  			if(numOfCoins < 0 || numOfCoins > totalFound) {
  				sender ! AssignWork(k, begin, workSize)
  				begin = begin + workSize
  			} else {
  				self ! Stop
  			}
  		}
  		case Stop => {
  			context.system.shutdown()
  		}
  	}
  }

  class Client(serverAddress: String, config: Config) extends Actor {
  	var server : ActorSelection = _
  	if(!serverAddress.isEmpty()){
  		server = context.actorSelection(serverAddress)
  		server ! ClientAvailiableNow("A remote client availiable now : " + self)
  	}
  	var serverback: ActorRef = _

  	val workerPerCore : Double = config.getDouble("WorkersPerCore")
  	val numOfWorkers : Int = Math.ceil(Runtime.getRuntime().availableProcessors() * workerPerCore).toInt
  	//println("Number Of worker :", numOfWorkers)
  	val workers = context.actorOf(RoundRobinPool(numOfWorkers).props(Props[Worker]),"worker")
  	var coinsFound : List[String] = new ArrayList[String]()
  	var numOfWorkFinish : Int = 0
  	def receive = {
  		case Start => {
  			sender ! ClientAvailiableNow("Local client availiable now :" + self)
  		}
  		case AssignWork(k , begin, workSize) => {
  			serverback = sender
  			//println("work assign to client" + self)
  			var block : Long = begin
  			var blockSize : Int = workSize / numOfWorkers
  			for(i <- 1 to numOfWorkers) {
  				//println("Assgin to wokers")
  				workers ! Mining(k, block, blockSize)
  				block = block + blockSize
  			}
  		}

  		case CoinsFoundByWoker(coins) => {
  			numOfWorkFinish = numOfWorkFinish + 1
  			if(!coins.isEmpty()){
  				coinsFound.addAll(coins)
  			}
  			//println("numOfWorkFinish : " + numOfWorkFinish + "  numOfWorkers : " + numOfWorkers)
  			if(numOfWorkFinish == numOfWorkers) {
  				serverback ! SendCoinsToServer(coinsFound)
  				numOfWorkFinish = 0
  				coinsFound = new ArrayList[String]()
  			}
  		}
  	}
  }

  class Worker extends Actor {
  		def receive = {
			case Mining(difficulty: Int, start: Long, worksize: Int)=>{
				//println(difficulty + " 0 prefix  start from" + start + " work size is " + worksize)
				sender ! CoinsFoundByWoker(mineBitCoints(difficulty, start, worksize))
			}			
		}

		def mineBitCoints(difficulty: Int, start: Long, worksize: Int): List[String] = {
			//println("Begin mine")
      		val id: String = "heziyang;"
      		var sb: StringBuilder = new StringBuilder(256)
      		for (i <- 1 to difficulty) {
        		sb.append(0)
      		}
      		for (i <- (difficulty + 1) to 256) {
        		sb.append('f')
        	}
      		var benchmark: String = sb.toString();
      		
      		//val hexToCmp: String = benchmark

      		var coinsFound = new ArrayList[String]()

      		for (i <- start to (start + worksize)) {
        		var nonce = id + i
        		var md = MessageDigest.getInstance("SHA-256")
        		md.update(nonce.getBytes("UTF-8"))
        		var digest = md.digest()
        		var digestHexRep = bytes2hex(digest)

        		if (digestHexRep <= benchmark) {
          			coinsFound.add(nonce + "\t" + digestHexRep)
        		}
      		}
      		return coinsFound
    	}

    	def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String = {
      		sep match {
        		case None => bytes.map("%02x".format(_)).mkString
        		case _ => bytes.map("%02x".format(_)).mkString(sep.get)
      		}
    	}

    	def hex2bytes(hex: String): Array[Byte] = {
      		hex.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
    	}
  }
}




