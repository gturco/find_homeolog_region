import MySQLdb
import MySQLdb.cursors
import pickle
from flatfeature import Bed


class mysql_syn_import():
    """docstring for qa_file """
    def __init__(self, db_name, organisms = 'org_org'):
        db=MySQLdb.connect(host="127.0.0.1", user="root", db= db_name)
        self.cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        
        table = syn_table_stmt = "CREATE TABLE IF NOT EXISTS %s (sfeat VARCHAR(20),\
                         qfeat VARCHAR(20), UNIQUE (sfeat, qfeat))" % organisms
        self.cursor.execute(table)
        
        self.file_name = organisms
        
    def pairs(self, dag_path):
        "grabs pairs from dag file path" 
        pairs_list = []
        for line in open(dag_path, 'r'):
            pairs = line.strip().split("\t")
            if len(pairs) == 2:
                pair = pairs[0], pairs[1]
                pairs_list.append(pair)
        return pairs_list
    
    def import_pairs_to_mysql(self, qfeat, sfeat):
        "puts qfeat and sfeat into mysql"                            
        insert_statement = "INSERT IGNORE INTO %s(sfeat,qfeat) VALUES('%s', '%s')" %(self.file_name, qfeat, sfeat) 
        self.cursor.execute(insert_statement)
        
        
    def org1_org1(self, dag_path):
        "run if you are running a syn_map against itself for example rice_rice, diffrent because needs repeats"
        pairs_list = self.pairs(dag_path)
        for pairs in pairs_list:
            qfeat, sfeat = pairs
            self.import_pairs_to_mysql(qfeat, sfeat)            
            stmt = "INSERT IGNORE INTO %s (qfeat,sfeat) VALUES('%s','%s')" %(self.file_name, qfeat, sfeat)
            self.cursor.execute(stmt)
            
        print 'dag_file sucessfully imported =)'

    def org1_org2(self, dag_path):
        "run if you are running two oganisms against one another ex sorg_rice"
        pairs_list = self.pairs(dag_path)
        for pairs in pairs_list:
            qfeat, sfeat = pairs
            self.import_pairs_to_mysql(qfeat, sfeat)
        
        print 'dag_file sucessfully imported =)'
            
            
    def import_bed_to_mysql(self, bed_path, dag_path):
        
        bed_table_stmt = "CREATE TABLE IF NOT EXISTS %s_bed (accn VARCHAR(20), start INTEGER,\
                        end INTEGER, Chr VARCHAR(2), Strand VARCHAR(2))" % self.file_name
        self.cursor.execute(bed_table_stmt)
        
        
        bed = Bed(bed_path)
        pairs_list = self.pairs(dag_path)
        for pairs in pairs_list:
            qfeat, sfeat = pairs        
            for gene in bed:
                if gene[3] == qfeat or gene[3] == sfeat:
                    insert_statement = "REPLACE INTO %s_bed VALUES('%s', %d, %d,'%s', '%s')" \
                                         % (self.file_name, gene[3],int(gene[1]),int(gene[2]),\
                                         gene[0],gene[5])
                    self.cursor.execute(insert_statement)
        print 'bed sucessfully imported =)'
            
            


# bob = mysql_syn_import('test', 'hi3')
# Bed('/Users/gturco/results/data/rice_v6.bed')
# bob.import_bed_to_mysql('/Users/gturco/results/data/rice_v6.bed' ,'/Users/gturco/results/data/rice_v6_rice_v6.pairs.txt')
# # #bob.org1_org1('/Users/gturco/results/data/rice_v6_rice_v6.pairs.txt')
# # gina = mysql_syn_map('test', 'hi2')
# # gina.org1_org2('/Users/gturco/results/data/rice_v6_sorghum_v1.pairs.txt')
