from __future__ import print_function
from BlockChain import BlockChain
blockchain = BlockChain()

def mine_a_block(sender, recepient, amount):
    last_block = blockchain.get_last_block
    last_proof = last_block.proof
    proof = blockchain.create_proof_of_work(last_proof)
    blockchain.create_new_transaction(sender = sender, recepient = recepient, amount = amount)
    last_hash = last_block.get_block_hash
    blockchain.create_new_block(proof, last_hash)


def main():    
    print(">>>> Before Mining . . .")
    print(blockchain.chain)

    #last_block = blockchain.get_last_block
    #last_proof = last_block.proof
    #proof = blockchain.create_proof_of_work(last_proof)

    ## Sender "0" means that this node has mined a new block
    ## For mining the Block(or finding the proof), we must be awarded with some
    ## amount (in our case this is 1)
    #blockchain.create_new_transaction(sender = "0", recepient = "address_x", amount = 1)
    #last_hash = last_block.get_block_hash
    #blockchain.create_new_block(proof, last_hash)

    mine_a_block("0", "address_x", 1)
    mine_a_block("manzur", "sam", 750)


    print(">>>> After Mining . . .")
    print(blockchain.chain)

    print ("------\n\n")

    for b in blockchain.chain:
        print(str(b.index) + " | " + str(b.proof) + " | " + str(b.previous_hash) + " | " + str(b.timestamp) + " | " + str(b.transactions))

if __name__ == "__main__":
    main()
