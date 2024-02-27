## QUESTIONS

### Omnipaxos Durability

- [X] Do we need to use OmniPaxos library to implement omnipaxos_durability ?
    
  - [ ] What functions do we need to use from the Omnipaxis library ?  
    - Our thought is closer to use the Omnipaxos Storage.

- [ ] How OmniPaxos is connected to the datastore and the node?

#### Implementation

- [ ] Is it ok to read from 0 to decided index into iter function? And if not how can we read all the length of the omnipaxos structure that we have?