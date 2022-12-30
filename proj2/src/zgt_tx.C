/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

//Modified at 6:35 PM 09/29/2016 by Jay D. Bodra. Search for "Fall 2016" to see the changes
// Fall 2016[jay]. Removed the TxType that was provided. Now is it initialized once in the constructor

extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort(long, char);   //commit/abort based on char value
extern void *process_read_write(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

//Fall 2016[jay]. Modified zgt_tx() in zgt_tx.h
zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
  this->lockmode = (char)' ';  //default
  this->Txtype = type; //Fall 2016[jay] R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1; //set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; //init to  an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. when creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    //Fall 2016[jay]. writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "\nT%d\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){

  int status_call=0;
  struct param *node = (struct param*)arg;// get tid and objno and count
  start_operation(node->tid,node->count); // starting operation with mutex lock
  zgt_tx *tx= get_tx(node->tid); // getting transaction id

  if(tx!=NULL)
  {
    if (tx->status==TR_ACTIVE){ // if  transaction active 
      status_call=1;
    }
    else if(tx->status==TR_WAIT){ // if transaction in wait
      status_call=2;
    }
    else if(tx->status==TR_END){ // if transaction in commit
      status_call=3; 
    }
    else if(tx->status==TR_ABORT){ // if transaction in abort
      status_call=4;
    }
  }

  else{
    printf("error in readtx"); // if transaction is null print error
    fflush(stdout);
    finish_operation(node->tid);
    pthread_exit(NULL);
  }

  switch(status_call){
    case 1:
      tx->set_lock(node->tid,1,node->obno,node->count,'S'); // when transaction is active set transaction lock with 'S' lockmode for share memory lock
      finish_operation(node->tid);  // finish operation with mutex unlock
      pthread_exit(NULL);   // exiting thread
      break;

    case 2:
      do_commit_abort(node->tid,TR_WAIT); // when transaction in wait call do_commit_abort()
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break;
    
    case 3:
      do_commit_abort(node->tid,TR_END); // when transaction in commit call do_commit_abort()
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break;
    case 4:
      do_commit_abort(node->tid,TR_ABORT); // when transaction in abort call do_commit_abort()
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break;

    default:
    break;

  }
  
  //do the operations for reading. Write your code
}


void *writetx(void *arg){ 
  
//do the operations for writing; similar to readTx
  int status_call=0;
  struct param *node = (struct param*)arg;// get tid and objno and count
  start_operation(node->tid,node->count); // starting operation with mutex lock
  zgt_tx *tx= get_tx(node->tid); // getting transaction id

  if(tx!=NULL)
  {
    if (tx->status==TR_ACTIVE){ // if  transaction active
      status_call=1;
    }
    else if(tx->status==TR_WAIT){ // if  transaction in wait 
      status_call=2;
    }
    else if(tx->status==TR_END){ // if  transaction in commit 
      status_call=3;
    }
    else if(tx->status==TR_ABORT){ // if  transaction in abort 
      status_call=4;
    }
  }

  else{
    printf("error in writetx"); // if transaction is null print error
    fflush(stdout);
    finish_operation(node->tid);
    pthread_exit(NULL);
  }

  switch(status_call){
    case 1:
      tx->set_lock(node->tid,1,node->obno,node->count,'X'); // when transaction is active set transaction lock with 'X' lockmode for exclusive lock
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break; 

    case 2:
      do_commit_abort(node->tid,TR_WAIT); // when transaction in wait call do_commit_abort()
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break;
    
    case 3:
      do_commit_abort(node->tid,TR_END); // when transaction in commit call do_commit_abort()
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break;
    case 4:
      do_commit_abort(node->tid,TR_ABORT); // when transaction in abort call do_commit_abort()
      finish_operation(node->tid); // finish operation with mutex unlock
      pthread_exit(NULL); // exiting thread
      break;

    default:
    break;

  }
  //do the operations for writing; similar to readTx. Write your code

}

//common method to process read/write: just a suggestion

void *process_read_write(long tid, long obno,  int count, char mode){

  
}

void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg;
  start_operation(node->tid,node->count); // starting operation with mutex lock
  zgt_tx *tx=get_tx(node->tid); // getting transaction id

  if(tx!=NULL){ // if transaction not null call do_commit_abort()
    do_commit_abort(node->tid,TR_ABORT);
    finish_operation(node->tid); // finish operation with mutex lock 
    pthread_exit(NULL); // exiting thread

  } 
  else{ // if transaction is null print error
    printf("eror in aborttx");
    fflush(stdout);
    finish_operation(node->tid);
    pthread_exit(NULL);
  }
  
  		// thread exit
}

void *committx(void *arg)
{
 
    
  struct param *node = (struct param*)arg;
  start_operation(node->tid,node->count); // starting operation with mutex lock
  zgt_tx *tx=get_tx(node->tid); // getting transaction id
 
  if(tx!=NULL){ // if transaction not null call do_commit_abort()
    do_commit_abort(node->tid,TR_END);
    finish_operation(node->tid); // finish operation with mutex lock 
    pthread_exit(NULL); // exiting thread

  } 
  else{ // if transaction is null print error
    printf("eror in committx");
    fflush(stdout);
    finish_operation(node->tid);
    pthread_exit(NULL);
  }
  		
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existant tx

void *do_commit_abort(long tid, char status){
  
  
  
  if(status==TR_END){ //if transaction is commit print the id in logfile
    fprintf(ZGT_Sh->logfile,"\nT%d              CommitTx              ",tid);
    fflush(ZGT_Sh->logfile);
  
  }
  if(status==TR_ABORT){ //if transaction is abort print the id in logfile
    fprintf(ZGT_Sh->logfile,"\nT%d              AbortTx               ",tid);
    fflush(ZGT_Sh->logfile);
  }

  zgt_tx *tx=get_tx(tid); // getting transaction ID

  if (tx==NULL){ //if transaction is null print error
      printf(" Error in do_commit_abort execution");
      fflush(stdout);
  }
    else{ //if transaction is not null free locks and remove transaction that are waiting for the object 
      tx->free_locks();
      tx->remove_tx();
      int tx_wait=tx->semno; // getting no of transaction that are waiting 
      int no_tx=zgt_nwait(tx_wait);
      int i=no_tx;
        if(no_tx>0){
          while(i>0){
            zgt_v(tx->semno); //releasing the semaphore in the transactions
            i=i-1;
          }
           // calling v operation on the transaction ID
          }
        

      }   
  

  // write your code
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if correct node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0); else -1
  
    //write your code
  int txs=0;
  int type_mode=0;
  zgt_tx *tx=get_tx(tid1); // get transaction id 
  zgt_p(0);
  zgt_hlink *prevtx=ZGT_Ht->find(sgno1,obno1); // get transaction from hash table that currently holding the object
  zgt_v(0);
  

  if(prevtx==NULL){ // no tansactionIDs has locked the objectID
    txs=1;
  }
  else if(prevtx->tid==tx->tid){  // locked by same transactionID
    txs=2;
  }
  else{ //locked by another transactionID and the current transactionID is not locked yet
    
    txs=3;
  }
 switch(txs){
    case 1:{
      int stat_add;
      zgt_p(0);
      stat_add=ZGT_Ht->add(tx,sgno1,obno1,lockmode1); // add the transaction to the hash table for locking 
      zgt_v(0);
      if (stat_add>=0){
          tx->perform_readWrite(tid1,obno1,lockmode1);
      }
      else{
        printf(" not able to add into hash table for lock\n");
        fflush(stdout);
      }
      
      } // calling perform_readwrite()
      break;

     case 2:
      tx->perform_readWrite(tid1,obno1,lockmode1); // calling perform_readwrite()
      break;

    case 3:{
      zgt_p(0); 
      zgt_hlink *curr_obj=ZGT_Ht->findt(tid1,sgno1,obno1); // get the object holding the transaction
      zgt_v(0);
      int no_waittx=zgt_nwait(prevtx->tid); // getting no of transactions that are waiting for previous transaction
      if(curr_obj==NULL){ // if no object is returned then no transaction is holding the object 
        if(lockmode1=='X'){ // if lockmode is write
          type_mode=1;
        }
        else if(prevtx->lockmode=='X' && lockmode1=='S'){ // if  previous lockmode is write and the current lockmode is read
          type_mode=2;
        }
        else if(prevtx->lockmode=='S' && lockmode1=='S'){ // if  previous lockmode is read and the current lockmode is read
          type_mode=3;
        }
        switch(type_mode){
          case 1: // when lockmode is write
            tx->obno = obno1; // sending the transaction to wait state by adding the previous transaction id to it semaphore
            tx->lockmode=lockmode1;
            tx->status= TR_WAIT;
            tx->setTx_semno(prevtx->tid,prevtx->tid);
            zgt_p(prevtx->tid);
            tx->obno=-1; // when the transaction become active call perform_readwrite()
            tx->lockmode=' ';
            tx->status=TR_ACTIVE;
            tx->perform_readWrite(tid1,obno1,lockmode1);
            zgt_v(prevtx->tid);
            break;

          case 2: // when  previous lockmode is write and the current lockmode is read
            tx->obno = obno1; // sending the transaction to wait state by adding the previous transaction id to it semaphore
            tx->lockmode=lockmode1;
            tx->status= TR_WAIT;
            tx->setTx_semno(prevtx->tid,prevtx->tid);
            zgt_p(prevtx->tid);
            tx->obno=-1; // when the transaction become active call perform_readwrite()
            tx->lockmode=' ';
            tx->status=TR_ACTIVE;
            tx->perform_readWrite(tid1,obno1,lockmode1);
            zgt_v(prevtx->tid);
            break;
          case 3: // when  previous lockmode is read and the current lockmode is read
             
            if(no_waittx>0){ //when the no of transactions that are waiting is greater than 0 put the transaction is queue
              tx->obno = obno1; // sending the transaction to wait state by adding the previous transaction id to it semaphore
              tx->lockmode=lockmode1;
              tx->status= TR_WAIT;
              tx->setTx_semno(prevtx->tid,prevtx->tid);
              zgt_p(prevtx->tid);
              tx->obno=-1; // when the transaction become active call perform_readwrite()
              tx->lockmode=' ';
              tx->status=TR_ACTIVE;
              tx->perform_readWrite(tid1,obno1,lockmode1);
              zgt_v(prevtx->tid);
            }
            else{
              status=TR_ACTIVE;
              prevtx->lockmode=lockmode1;
              tx->perform_readWrite(tid1,obno1,lockmode1); // when there is no wait queue just call  perform_readwrite()
            }
            break;
            default:
            break;
        }
            }
      else{
        status=TR_ACTIVE;
        prevtx->lockmode=lockmode1;
        tx->perform_readWrite(tid1,obno1,lockmode1); // when the is returned call perform_readwrite()
      }
    }
    break;

    default:
      break;

  }
  return(0);
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink* temp = head;  //first obj of tx
  
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      fprintf(ZGT_Sh->logfile, "%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(ZGT_Sh->logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  //2016: not used
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the acutual read/write operation
// based  on the lockmode

void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){
  
  int i=0;
  int j=0;
  int sleep=0;
  int object_value=ZGT_Sh->objarray[obno]->value; // initalizing object value
  if (lockmode=='X'){
    ZGT_Sh->objarray[obno]->value=object_value+1; // incrementing the object value if the lockmode is write
    fprintf(ZGT_Sh->logfile, "\nT%d               writeTx        %d:%d:%d          writeLock          Granted                 %c\n",this->tid, obno,ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid],this->status);
    fflush(ZGT_Sh->logfile);
    while(i<ZGT_Sh->optime[tid]*25){ // making sleep to write into logile
      i=i+1;
      sleep=sleep+1;
    }
  }
  if(lockmode=='S'){
    ZGT_Sh->objarray[obno]->value=object_value-1; // decrementing the object value if the lockmode is read
    fprintf(ZGT_Sh->logfile, "\nT%d                readTx        %d:%d:%d         ReadLock            Granted                  %c\n",this->tid, obno,ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
    fflush(ZGT_Sh->logfile);
    while(j<ZGT_Sh->optime[tid]*15){ // making sleep to write into logile
      j=j+1;
      sleep=sleep+1;
    }
    
  }
  //write your code

}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}


