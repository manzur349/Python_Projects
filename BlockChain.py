from Block import Block

class BlockChain(object):
    def __init__(self):
        self.chain = []
        self.current_node_transactions = []
        self.create_genesis_block()
        self.nodes = set()

    def create_genesis_block(self):
        self.create_new_block(proof = 0, previous_hash = 0)

    def create_new_block(self, proof, previous_hash):
        block = Block(len(self.chain), proof = proof, previous_hash = previous_hash, transactions = self.current_node_transactions)
        self.current_node_transactions=[]
        self.chain.append(block)
        return block

    def create_new_transaction(self, sender, recipient, amount):
        self.current_node_transactions.append({"sender" : sender, "recipient" : recipient, "amount" : amount})
        return self.get_last_block.index + 1 #returning new block's index where transaction will be stored
    
    def create_node(self, address):
        self.nodes.add(address)
        return True
        
    @staticmethod
    def create_proof_of_work(previous_proof):
        """
        Generate "Proof Of Work"
        A very simple 'Proof of Work' Algorithm -
        -> Find a number such that, sum of the number and previous POW number is divisible
        by 7
        """
        proof = previous_proof + 1
        while (proof + previous_proof) % 7 != 0:
            proof += 1
        return proof
    
    @staticmethod
    def get_block_object_from_block_data(block_data):
        return Block(block_data["index"], 
            block_data["proof"], 
            block_data["previous_hash"], 
            block_data["transactions"], 
            timestamp=block_data["timestamp"]
        )

    @property
    def get_last_block(self):
        return self.chain[-1]

    @property
    def get_serialized_chain(self):
        return [vars(block) for block in self.chain]
    
    def mine_block(self, miner_address):
        # Sender "0" means that this node has mined a new block
        # For mining the Block(or finding the proof), we must be awarded with some amount (in our case this is 1)
        self.create_new_transaction(sender="0", recipient=miner_address, amount=1,)
        last_block = self.get_last_block
        last_proof = last_block.proof
        proof = self.create_proof_of_work(last_proof)
        last_hash = last_block.get_block_hash
        block = self.create_new_block(proof, last_hash)
        return vars(block)
