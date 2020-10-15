#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 


open System
// open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic

type Gossip =
    |SetNbrs of IActorRef[] 
    |StartGossip of String
    |ReportMsgRecvd of String
    |SetValues of int * IActorRef[] * int
    |PushSum of float * float
    |StartPushSum of int

let r  = System.Random()

let system = ActorSystem.Create("System")

let mutable conTime=0

let Listener (mailbox:Actor<_>) = 

    let mutable noOfMsg = 0
    let mutable startTime = 0
    let mutable totNodes =0
    let mutable allNodes:IActorRef[] = [||]

    let rec loop() = actor {
            let! message = mailbox.Receive()
            match message with 
            //gossip convergence
            | ReportMsgRecvd message ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                noOfMsg <- noOfMsg + 1
                if noOfMsg = totNodes then
                    printfn "Time for convergence: %A ms" (endTime-startTime)
                    conTime <-endTime-startTime
                    // Environment.Exit 0
                else
                    //printfn "numofpeople = %d" totNodes
                    let newStart= r.Next(0,allNodes.Length)
                    allNodes.[newStart] <! StartGossip("Hello")

       
          
            | SetValues (strtTime,nodesRef,totNds) ->
                startTime <-strtTime
                allNodes <- nodesRef
                totNodes <-totNds
                
            | _->()

            return! loop()
        }
    loop()

    

let Node listener nodeNum (mailbox:Actor<_>)  =

    
    let mutable numMsgHeard = 0 
    let mutable nbrs:IActorRef[]=[||]

    //used for push sum
    let mutable sum1= nodeNum |> float
    let mutable weight = 1.0
    let mutable termRound = 1.0
    let mutable flag = 0
    let mutable counter = 1
    let mutable ratiochange = 0.0
    let mutable ratio1 = 0.0
    let mutable ratio2 = 0.0
    let mutable ratio3 = 0.0
    let ratiolimit = 10.0**(-10.0)


    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |StartPushSum ind->
            let index = r.Next(0,nbrs.Length)
            sum1<-sum1/2.0
            weight<-weight/2.0
            //printfn "%f %f" sum1 weight
            nbrs.[index] <! PushSum(sum1,weight)

        |PushSum (s,w)->
            numMsgHeard<- numMsgHeard+1 //remove this line
            sum1<-sum1+s
            sum1<-sum1/2.0
            weight<-weight+w
            weight<-weight/2.0
            //printfn "%A %A" sum1 weight
            
            if(flag = 0) then
                if(counter = 1) then
                    ratio1<-sum1/weight
                else if(counter = 2) then
                    ratio2<-sum1/weight
                else if(counter = 3) then
                    flag<-1

                counter<-counter+1

            ratio3<-sum1/weight
            //printfn "%f" ratio3

            if(abs(ratio2-ratio3)<=ratiochange) then 
            //if(numMsgHeard = 15) then
                    //   ratio3<-sum1/weight
                    //   printfn "%f" ratio3
                    //   if(ratio3-ratio1<=ratiochange) then
                    //     printfn"NODE FINISHED: %A" nodeNum
                    //     //listener <! ReportMsgRecvd(msg)
                    //   else
                    //     ratio1<-ratio2
                    //     ratio2<-ratio3
                    printfn "%f %f" ratio1 ratio3
                    printfn"NODE FINISHED: %A" nodeNum
            else
                      ratio1<-ratio2
                      ratio2<-ratio3
                      let index= r.Next(0,nbrs.Length)
                      printfn "%f" ratio2
                      nbrs.[index] <! PushSum(sum1,weight)
            //let index = r.Next(0,nbrs.Length)
            //nbrs.[index] <! PushSum(sum1,weight)
            //printfn "%A %A" s w 
            //printfn "%A" weight

        | SetNbrs nArray->
                nbrs<-nArray
                // printfn"narray: %A"nbrs
              //  printfn "Coming %d"nodeNum  

        | StartGossip msg ->
                numMsgHeard<- numMsgHeard+1
                //printfn "%s %i" (Actor.Context.Self.ToString()) numMsgHeard
                if(numMsgHeard = 10) then
                      printfn"NODE FINISHED: %A" nodeNum
                      listener <! ReportMsgRecvd(msg)
                else
                      let index= r.Next(0,nbrs.Length)
                    //   printfn"index:%A "index 
                      nbrs.[index] <! StartGossip(msg)
        | _-> ()

        return! loop()
    }
    loop()


//Code to compute CPU Time and Real Time
    // let coreCount = Environment.ProcessorCount
    // let timer = Stopwatch()
    // timer.Start()
    // let pId = Process.GetCurrentProcess()
    // let cpu_time_stamp = pId.TotalProcessorTime
module prtcl=

        let callPrtcl algo numN nodeArr=
            (nodeArr : _ array)|>ignore
            if algo="gossip" then 
                let starter= r.Next(0,numN-1)
                printfn "Starting Protocol Gossip with: %A" starter
                nodeArr.[starter]<!StartGossip("Hello")
            if algo="push-sum" then
                printfn"Algo is pushmsum"
                let starter= r.Next(0,numN-1)
                nodeArr.[starter]<!StartPushSum(starter)

            

module tplgy=
                
        // let removeelement arr v =
        //     (Array.choose (fun elem -> 
        //     if elem > v || elem < v then
        //         Some(elem)
        //     else
        //         None) arr)

        // let rec remove_first pred lst =
        //     match lst with
        //     | h::t when pred h -> t
        //     | h::t -> h::remove_first pred t
        //     | _ -> []

        

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
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
                elif i=(numN-1) then 
                    nbrArray<-nodeArray.[0..(numN-2)]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
                else
                    nbrArray<-Array.append nodeArray.[0..i-1] nodeArray.[i+1..numN-1]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

            // for i in [0..numN-1] do
            //     nodeArray.[i]<!SetNbrs(nodeArray)

        
            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray

            // let allN=[0..numN-1]
            // allN |> List.iter (fun item ->
            //     // let adj = allN |> remove_first (fun x -> x=item)
            //     printfn "Removed %A"item
            // )
            

        // let getFullnbrs allN pid=
        //      allN |> remove_first (fun x -> x=pid)
            
        

        let build2D numN algo=
            printfn "Inside 2D"
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

            // let aa=[|1..numN|]
            // let mutable aa2=Array.empty
            // printfn "aa is: %A"aa
            // let res = removeelement aa 3
            // printfn "res: %A" res 
        
            

            // for i in [0..numN-1] do
            //     if i=0 then
            //         aa2<-aa.[1..1]
            //         printfn "i is:%A a2 is: %A"i aa2
            //     elif i=(numN-1) then 
            //         aa2<-aa.[(numN-2)..(numN-2)]
            //         printfn "i is:%A a2 is: %A"i aa2
            //     else
            //         aa2<-aa.[i-1..i+1]
            //         printfn "i is:%A a2 is: %A"i aa2

            let mutable nbrArray:IActorRef[]=Array.empty
            let mutable a3:IActorRef[] = [||]
            
            // printfn"NodeARRay: %A"nodeArray
            // a3<-removeelement nodeArray 2
            // printfn"resNd %A"a3
            

            for i in [0..numNM-1] do
                if i=0 then
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i+rCount..i+rCount]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+rCount..i+rCount]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
                
                elif i=numNM-rCount then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i<rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
               

                elif i>numNM-rCount && i<numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif (i+1)%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
               
                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

            // for i in [0..numN-1] do
            //     nodeArray.[i]<!SetNbrs(nodeArray)
            
        
            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray


        let buildLine numN algo=
            printfn "Line"
            let listener= 
                Listener
                    |> spawn system "listener"

            let nodeArray = Array.zeroCreate(numN)
            
            for i in [0..numN-1] do
                nodeArray.[i]<- Node listener (i+1)
                                    |> spawn system ("Node"+string(i))

            let aa=[|1..numN|]
            let mutable aa2=Array.empty
            printfn "aa is: %A"aa
            // let res = removeelement aa 3
            // printfn "res: %A" res 
        
            

            // for i in [0..numN-1] do
            //     if i=0 then
            //         aa2<-aa.[1..1]
            //         printfn "i is:%A a2 is: %A"i aa2
            //     elif i=(numN-1) then 
            //         aa2<-aa.[(numN-2)..(numN-2)]
            //         printfn "i is:%A a2 is: %A"i aa2
            //     else
            //         aa2<-aa.[i-1..i+1]
            //         printfn "i is:%A a2 is: %A"i aa2

            let mutable nbrArray:IActorRef[]=Array.empty
            let mutable a3:IActorRef[] = [||]
            
            // printfn"NodeARRay: %A"nodeArray
            // a3<-removeelement nodeArray 2
            // printfn"resNd %A"a3

            for i in [0..numN-1] do
                if i=0 then
                    nbrArray<-nodeArray.[1..1]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
                elif i=(numN-1) then 
                    nbrArray<-nodeArray.[(numN-2)..(numN-2)]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

            // for i in [0..numN-1] do
            //     nodeArray.[i]<!SetNbrs(nodeArray)
            
        
            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray
        
        let buildImp2D numN algo=
            printfn "Inside Imp2D:"
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
            let mutable a3:IActorRef[] = [||]
            


            for i in [0..numNM-1] do
                if i=0 then
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i+1|]; [|i+rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
                
                elif i=numNM-rCount then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount]
                    let elemremove = Array.concat [[|i|]; [|i+1|]; [|i-rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i=numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i-rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i<rCount-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount] 
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+1|]; [|i+rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr]
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
               

                elif i>numNM-rCount && i<numNM-1 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+1|]; [|i-rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif i%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i+1..i+1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i+1|]; [|i-rCount|]; [|i+rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

                elif (i+1)%rCount = 0 then 
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i-rCount|]; [|i+rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)
               
                else
                    nbrArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1] 
                    nbrArray<-Array.append nbrArray nodeArray.[i-rCount..i-rCount] 
                    nbrArray<-Array.append nbrArray nodeArray.[i+rCount..i+rCount]
                    let elemremove = Array.concat [[|i|]; [|i-1|]; [|i+1|]; [|i-rCount|]; [|i+rCount|]]
                    let mutable selectrand = [|0..8|]
                    for i in 0..elemremove.Length-1 do
                        selectrand<-Array.filter (fun x -> x <> elemremove.[i])selectrand
                    let randnbr=selectrand.[r.Next(selectrand.Length)]
                    nbrArray<-Array.append nbrArray nodeArray.[randnbr..randnbr] 
                    printfn "i is:%A a2 is: %A"i nbrArray
                    nodeArray.[i]<!SetNbrs(nbrArray)

            // for i in [0..numN-1] do
            //     nodeArray.[i]<!SetNbrs(nodeArray)
            
        
            listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,numN)

            prtcl.callPrtcl algo numN nodeArray







        let createTopology top numN algo=
           if top="full" then buildFull numN algo
           elif top="2D" then build2D numN algo
           elif top="line" then buildLine numN algo
           elif top="imp2D" then buildImp2D numN algo
           else printfn"Wrong Argument"
             


               


//Mainmodule that spawns the boss and various actors 
module mainModule=

        //Receiving input from user and storing it in final and k
        let args : string array = fsi.CommandLineArgs |> Array.tail
        let mutable numNodes=args.[0] |> int
        let topology=args.[1] |> string
        let algo=args.[2] |>string
        
        

        // let tot=6
        // let a1=[|1..tot|]
        // // let a2=a1.[1..1]
        // let mutable a2=Array.empty
        // printfn "a1 is: %A"a1
        
        // for i in [1..tot] do
        //     if i=1 then
        //         a2<-a1.[i..i]
        //         printfn "i is:%A a2 is: %A"i a2
        //     elif i=tot then 
        //         a2<-a1.[tot-2..tot-2]
        //         printfn "i is:%A a2 is: %A"i a2
        //     else
        //         a2<-a1.[i-2..i]
        //         printfn "i is:%A a2 is: %A"i a2
           
        


        printfn "ARG1: %A" numNodes
        printfn "ARG2: %A" topology
        printfn "ARG3: %A" algo

        

        // let listener= 
        //     Listener
        //         |> spawn system "listener"  

        // if topology="full" then 
        //     printfn"Inside full"
        //     let nodeArray= Array.zeroCreate (numNodes+1)
        //     printfn"Inside full2"
        //     for i in [0..numNodes] do
        //         printfn"Inside full %A"i
        //         nodeArray.[i]<- Node listener 10 (i+1)
        //                             |> spawn system ("Node"+string(i))
        //     for i in [0..numNodes] do
        //       nodeArray.[i]<!SetNbrs(nodeArray)
        //     let leader = r.Next(0,numNodes)
        //     if algo="gossip" then
        //         listener<!RecordtotNodes(numNodes)
        //         listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray)
        //         printfn "Starting Protocol Gossip"
        //         nodeArray.[leader]<!StartGossip("Hello")

        tplgy.createTopology topology numNodes algo
      

        
        // printf"CoNT TIME: %A ms"conTime
        System.Console.ReadLine() |> ignore