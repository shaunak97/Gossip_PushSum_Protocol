GOSSIP SIMULATOR

 Gossip type algorithms can be used both for group commu- nication and for aggregate computation. The goal of this project is to determine the convergence of such algorithms through a simulator based on actors written in F#. Since actors in F# are fully asynchronous, the particular type of Gossip implemented is the so called Asynchronous Gossip.

TEAM MEMBERS:

    BHARATH SHANKAR, UFID: 9841-4098
    SHAUNAK SOMPURA, UFID: 9911-2362

WHAT IS RUNNING(ALGORITHMS & TOPOLOGIES):

The program prints the convergence time of all nodes for the following algorithms and topologies.
For Gossip it ensures all nodes hear the rumour more than 10 times.
For push-sum, all nodes' ratio of s/w should not have changed more than 10^-10 in 3 consecutive rounds.

Gossip:

    Full Network
    Line
    2D Grid
    Imperfect 2D

Push-sum:

    Full Network
    Line
    2D Grid
    Imperfect 2D

HOW TO RUN:

1) Navigate to the folder with the file project2.fsx
2) Run the following command on the terminal:

        dotnet fsi --langversion:preview proj2bf.fs <numNodes> <topology> <algorithm>

    Where numNodes is the number of actors involved (for 2D based topologies you can round up until you
    get a square), topology is one of full, 2D, line, imp2D, algorithm is one of gossip, push-sum.

LARGEST NETWORK DEALT WITH:

Gossip

    Full Network: 50000
    Line: 5000
    2D Grid: 50000
    Imperfect 2D: 20000

Push-sum

    Full Network: 5000
    Line: 64
    2D Grid: 128
    Imperfect 2D: 512