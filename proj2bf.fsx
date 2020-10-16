#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 


open System
open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic

type Gossip =
    |SetNbrs of IActorRef[] 
    |BeginGossip of String
    |ConvMsgGossip of String
    |SetValues of int * IActorRef[] * int
    |PushSum of float * float
    |BeginPushSum of int
    |ConvPushSum of float * float

let r  = System.Random()

let timer = Stopwatch()

let system = ActorSystem.Create("System")

let mutable conTime=0

//Defining the listener
let Listener (mailbox:Actor<_>) = 

    let mutable noOfMsg = 0
    let mutable noOfNde = 0
    let mutable startTime = 0
    let mutable totNodes =0
    let mutable allNds:IActorRef[] = [||]

    let rec loop() = actor {

            let! message = mailbox.Receive()
            match message with 

//Checking for Gossip convergence
            | ConvMsgGossip message ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                noOfMsg <- noOfMsg + 1

                if noOfMsg = totNodes then
                    let rTime = timer.ElapsedMilliseconds
                    printfn "Time for Convergence from timer: %A ms" rTime
                    printfn "Time for Convergence from System Time: %A ms" (endTime-startTime)
                    conTime <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart= r.Next(0,allNds.Length)
                    allNds.[newStart] <! BeginGossip("Hello")


//Checking for Push Sum convergence
            | ConvPushSum (s,w) ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                noOfNde <- noOfNde + 1

                if noOfNde = totNodes then
                    let rTime = timer.ElapsedMilliseconds
                    printfn "Time for Convergence from timer: %A ms" rTime
                    printfn "Time for Convergence from System Time: %A ms" (endTime-startTime)
                    conTime <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart=r.Next(0,allNds.Length)
                    allNds.[newStart] <! PushSum(s,w)
          
            | SetValues (strtTime,nodesRef,totNds) ->
                startTime <-strtTime
                allNds <- nodesRef
                totNodes <-totNds
                
            | _->()

            return! loop()
        }
    loop()

    
//Defining Actor(Node)
let Node listener nodeNum (mailbox:Actor<_>)  =

    let mutable numMsgHeard = 0 
    let mutable nbrs:IActorRef[]=[||]

    let mutable sum1= nodeNum |> float
    let mutable weight = 1.0
    let mutable termRound = 1.0
    let mutable flag = 0
    let mutable counter = 1
    let mutable ratio1 = 0.0
    let mutable ratio2 = 0.0
    let mutable ratio3 = 0.0
    let mutable ratio4 = 0.0
    let mutable convflag = 0
    let ratiolimit = 10.0**(-10.0)
    
    let rec loop() = actor {

        let! message = mailbox.Receive()
        match message with 

//Starting Push Sum with random node
        |BeginPushSum ind->
            let index = r.Next(0,nbrs.Length)
            let sn = index |> float
            nbrs.[index] <! PushSum(sn,1.0)

//Push Sum Algorithm
        |PushSum (s,w)->
            if(convflag = 1) then
                let index = r.Next(0,nbrs.Length)
                nbrs.[index] <! PushSum(s,w)
            
            if(flag = 0) then
                if(counter = 1) then
                    ratio1<-sum1/weight
                else if(counter = 2) then
                    ratio2<-sum1/weight
                else if(counter = 3) then
                    ratio3<-sum1/weight
                    flag<-1

                counter<-counter+1

            sum1<-sum1+s
            sum1<-sum1/2.0
            weight<-weight+w
            weight<-weight/2.0
            
            ratio4<-sum1/weight
           
            if(flag=0) then
                let index= r.Next(0,nbrs.Length)
                nbrs.[index] <! PushSum(sum1,weight)                

            
            if(abs(ratio1-ratio4)<=ratiolimit && convflag=0) then 
                      convflag<-1
                      listener <! ConvPushSum(sum1,weight)

            else
                      ratio1<-ratio2
                      ratio2<-ratio3
                      ratio3<-ratio4
                      let index= r.Next(0,nbrs.Length)
                      nbrs.[index] <! PushSum(sum1,weight)

        | SetNbrs nArray->
                nbrs<-nArray

//Gossip Algorithm
        | BeginGossip msg ->

                numMsgHeard<- numMsgHeard+1

                if(numMsgHeard = 10) then
                      listener <! ConvMsgGossip(msg)

                else
                      let index= r.Next(0,nbrs.Length)
                      nbrs.[index] <! BeginGossip(msg)
        | _-> ()

        return! loop()
    }
    loop()

module prtcl=

//Calling the Algorithm
        let callPrtcl algo numN nodeArr=
            (nodeArr : _ array)|>ignore

            if algo="gossip" then 
                let starter= r.Next(0,numN-1)
                nodeArr.[starter]<!BeginGossip("Hello")

            elif algo="push-sum" then
                let starter= r.Next(0,numN-1)
                nodeArr.[starter]<!BeginPushSum(starter)

            else
                printfn"Wrong Argument!"

            

module tplgy=
 
 //Building Full Topology
        let buildFull numN algo=
            let listener= 
                Listener
                    |> spawn system "listener"

            let nodeArray = Array.zeroCreate(numN)

            let mutable nbrArray:IActorRef[]=Array.empty
            
            for i in [0..numN-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))
            
            for i in [0..numN-1] do
                if i=0 then
                    nbrArray<-nodeArray.[1..numN-1]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=(numN-1) then 
                    nbrArray<-nodeArray.[0..(numN-2)]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                else
                    nbrArray<-Array.append nodeArray.[0..i-1] nodeArray.[i+1..numN-1]
                    nodeArray.[i]<!SetNbrs(nbrArray)
           
            timer.Start()
                   
            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray

//Building 2D Grid Topology
        let build2D numN algo=
            let listener= 
                Listener
                    |> spawn system "listener"
            
            let rCount=int(round (sqrt (float numN)))
            let numNMod=rCount*rCount
            let numNM=int(numNMod)

            let nodeArray = Array.zeroCreate(numNM)
            
            for i in [0..numNM-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable nbrArray:IActorRef[]=Array.empty

            for i in [0..numNM-1] do
                if i=0 then
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i+rCount..i+rCount]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+rCount..i+rCount]
                    nodeArray.[i]<!SetNbrs(nbrArray)
                
                elif i=numNM-rCount then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i<rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    nodeArray.[i]<!SetNbrs(nbrArray)
               

                elif i>numNM-rCount && i<numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount] 
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif (i+1)%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    nodeArray.[i]<!SetNbrs(nbrArray)
               
                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    nodeArray.[i]<!SetNbrs(nbrArray)
            
            timer.Start()

            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numNM)

            prtcl.callPrtcl algo numN nodeArray

//Building Line Topology
        let buildLine numN algo=

            let listener= 
                Listener
                    |> spawn system "listener"

            let nodeArray = Array.zeroCreate(numN)
            
            for i in [0..numN-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable nbrArray:IActorRef[]=Array.empty

            for i in [0..numN-1] do
                if i=0 then
                    nbrArray<-nodeArray.[1..1]    
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=(numN-1) then 
                    nbrArray<-nodeArray.[(numN-2)..(numN-2)]                   
                    nodeArray.[i]<!SetNbrs(nbrArray)

                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1]            
                    nodeArray.[i]<!SetNbrs(nbrArray)
            
            timer.Start()

            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray
        
//Building Imperfect 2D Grid Topology
        let buildImp2D numN algo=

            let listener= 
                Listener
                    |> spawn system "listener"

            let rCount=int(round (sqrt (float numN)))
            let numNMod=rCount*rCount
            let numNM=int(numNMod)

            let nodeArray = Array.zeroCreate(numNM)
            
            for i in [0..numNM-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable nbrArray:IActorRef[]=Array.empty
          
            for i in [0..numNM-1] do
                if i=0 then
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i+1|]; [|i+rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    nodeArray.[i]<!SetNbrs(nbrArray)
                
                elif i=numNM-rCount then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount]
                    let elemremove = Array.concat [[|i|]; [|i+1|]; [|i-rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i-rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i<rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+1|]; [|i+rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    nodeArray.[i]<!SetNbrs(nbrArray)
               

                elif i>numNM-rCount && i<numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+1|]; [|i-rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i+1|]; [|i-rCount|]; [|i+rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif (i+1)%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i-rCount|]; [|i+rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    nodeArray.[i]<!SetNbrs(nbrArray)
               
                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+1|]; [|i-rCount|]; [|i+rCount|]]
                    let mutable selectrand = [|0..numNM-1|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand    
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    nodeArray.[i]<!SetNbrs(nbrArray)
            
            timer.Start()

            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numNM)

            prtcl.callPrtcl algo numN nodeArray

//Topology selection case
        let createTopology top numN algo=
           if top="full" then buildFull numN algo
           elif top="2D" then build2D numN algo
           elif top="line" then buildLine numN algo
           elif top="imp2D" then buildImp2D numN algo
           else printfn"Wrong Argument!"
             
module mainModule=

//Input
        let args : string array = fsi.CommandLineArgs |> Array.tail
        let mutable numNodes=args.[0] |> int
        let topology=args.[1] |> string
        let algo=args.[2] |>string
        
        tplgy.createTopology topology numNodes algo
   
        System.Console.ReadLine() |> ignore
