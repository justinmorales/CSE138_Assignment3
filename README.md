- CSE138_Assignment3
  Team name: JTJ
- Jun Hayashida: juhayash
- Justin Morales: jujmoral
- Tyler Fong: Tyrfong

Acknowledgements
================
* TA: Cheng-Wei Ching
* Tutor: Albert Lee

Citations
=========


Team Contributions
==================
* Jun Hayashida - Contributed to the design and implementation of vector clock operation to ensure maintaining eventual consistency across the distribued system. Involved in testing the code to identify and rectify errors, ensuring the system;s reliablility and correctness. Played a role in enhancing the readability and efficiency of the code, making it more maintainable and performant.
* Justin Morales - Designed the key/value store routes and how they function, designed the broadcasting mechanism we are using, designed
 the vector clock functionality,designed the view routes and their funstionality, designed the mechanism of how the replicas discover
 eachother in a system, designed the mechanism that checks if a replica is out of date, designed a mechanism for replicas to discover
 if another replica has been downed or is not responding so that it may be removed from the system. Wrote the Mechnism Description.
* Tyler Fong - N/A

Mechanism Description
==================
Describe how your system tracks causal dependencies, and how your system detects when a replica goes down.

The mechanism we use is very simple. If a replica goes down, the only reason they would need to know if a replica was down is in the event 
of a change in the key/value store, so in the event of a broadcast, they will know that the replica went down to then remove it from their 
view. The way that our system tracks causal dependencies is through a vector clock. When an event happens inside of the replica such a put 
or delete, then the vector clock for that replica is incremented. The vector clock is checked for correctness before and after the update 
of the vector clock to account for special circumstances. Durring a broadcast of a change the vector clock is send along with the data and 
the other replicas then compare their clock with the recieved clock. In general if the clock received is greater that the clock that the 
replica has, then the causal dependencies have not been satisfied. To fix quickly, you can shut down the replica and start it back up again. 
The replica then should be back up-to-date with the most updated replica.



Testing
==================
In order for the replicas to detect eachother successfully, there must be a short waiting period between the replicas starting for them to 
initialize. Please account for this by leaving a few second sleep during testing between the start of each replica. Thank you.
