import MySQLdb
from flatfeature import Bed

def remove_retined_homologs(syn_map1, syn_map2, bed_table_name, cursor): 
    "Creates a new table containg only genes that are not retained between both speices"    
    params = {'syn_map1':syn_map1, 'syn_map2': syn_map2, 'bed_table_name': bed_table_name}  
    grab_unique_genes = "CREATE TABLE IF NOT EXISTS non_retained AS\
                         (SELECT %(syn_map2)s.sfeat, %(bed_table_name)s.start, %(bed_table_name)s.end, \
                         %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                         FROM %(syn_map2)s , %(bed_table_name)s \
                         WHERE %(syn_map2)s.sfeat NOT IN \
                         (SELECT %(syn_map1)s.sfeat FROM %(syn_map1)s \
                         WHERE %(syn_map2)s.sfeat = %(syn_map1)s.sfeat)\
                         AND %(syn_map2)s.sfeat = %(bed_table_name)s.accn)" % params
    cursor.execute(grab_unique_genes)

def grab_genes_to_left(syn_map1, bed_table_name, cursor):
    "Creates tables left_genes_raw and left_genes, which contain a sfeat and\
    the closest gene to the left of it"
    params = {'syn_map1':syn_map1, 'bed_table_name': bed_table_name}
    left_genes_raw =  "CREATE TABLE  IF NOT EXISTS left_genes_raw AS\
                        (SELECT  %(syn_map1)s.qfeat as left_homolog , %(bed_table_name)s.accn as \
                        left_gene,  %(bed_table_name)s.end as left_gene_end, \
                        non_retained.start as sfeat_start , non_retained.sfeat , %(bed_table_name)s.strand\
                        FROM %(bed_table_name)s , %(syn_map1)s, non_retained \
                        WHERE non_retained.chr = %(bed_table_name)s.chr \
                        AND non_retained.strand = %(bed_table_name)s.strand \
                        AND %(bed_table_name)s.accn = %(syn_map1)s.sfeat \
                        AND %(bed_table_name)s.end < non_retained.start)" %params
    cursor.execute(left_genes_raw)
    
    left_genes_grouped = "CREATE TABLE  IF NOT EXISTS left_genes AS(SELECT * FROM \
                                 (SELECT * FROM left_genes_raw ORDER BY left_genes_raw.left_gene_end  DESC) \
                                 as left_genes_raw_tmp GROUP BY left_genes_raw_tmp.sfeat)"
    cursor.execute(left_genes_grouped)

def grab_genes_to_right(syn_map1, bed_table_name, cursor):
    "Creates tables right_genes_raw and right_genes, which contain a sfeat and\
      the closest gene to the right of it"
    params = {'syn_map1':syn_map1, 'bed_table_name': bed_table_name}
    right_genes_raw =  "CREATE TABLE IF NOT EXISTS right_genes_raw AS\
                            (SELECT  %(syn_map1)s.qfeat as right_homolog, %(bed_table_name)s.accn as \
                            right_gene, %(bed_table_name)s.start as right_gene_start, \
                            non_retained.end as sfeat_end, non_retained.sfeat , %(bed_table_name)s.strand \
                            FROM %(bed_table_name)s , %(syn_map1)s, non_retained \
                            WHERE non_retained.chr = %(bed_table_name)s.chr \
                            AND non_retained.strand = %(bed_table_name)s.strand \
                            AND %(bed_table_name)s.accn = %(syn_map1)s.sfeat \
                            AND %(bed_table_name)s.start > non_retained.end)" %params
    cursor.execute(right_genes_raw)
        
    right_genes_grouped_ordered = "CREATE TABLE IF NOT EXISTS right_genes AS(SELECT * From \
                        (SELECT * FROM right_genes_raw ORDER BY right_genes_raw.right_gene_start ASC) as right_genes_raw_tmp\
                        GROUP BY right_genes_raw_tmp.sfeat)"
    cursor.execute(right_genes_grouped_ordered)

def merge_left_right_gene_tables(syn_map1, bed_table_name, cursor):
    "creates a merged table of the sfeat and the gene to the left and right of it"
    merge = "CREATE TABLE merge_genes as(SELECT right_genes.sfeat , left_genes.left_gene_end, right_genes.right_gene_start, \
             (right_genes.right_gene_start - left_genes.left_gene_end) as difference,\
             left_genes.left_gene, right_genes.right_gene , right_genes.strand \
             FROM left_genes , right_genes \
             WHERE left_genes.sfeat = right_genes.sfeat)"
    cursor.execute(merge)
    
    add_accn_to_table = "ALTER TABLE merge_genes \
                         ADD accn INTEGER AUTO_INCREMENT NOT NULL FIRST, \
                         ADD PRIMARY KEY(accn)"
    cursor.execute(add_accn_to_table)

def create_region_table(bed_table_name , syn_map1, cursor):
    params = {'bed_table_name': bed_table_name, 'syn_map1' : syn_map1}
    # left_region = "CREATE TABLE IF NOT EXISTS left_region AS\
    #                (SELECT left_genes.left_homolog , %(bed_table_name)s.start as Lstart, \
    #                %(bed_table_name)s.end as Lend, left_genes.sfeat , \
    #                %(bed_table_name)s.chr , %(bed_table_name)s.strand  \
    #                FROM left_genes, %(bed_table_name)s \
    #                WHERE left_genes.left_homolog = %(bed_table_name)s.accn)" %params
    #cursor.execute(left_region)
    
    left_region_new = "CREATE TABLE left_region as(SELECT DISTINCT left_genes.* , %(bed_table_name)s.start , %(bed_table_name)s.end , %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                       FROM (SELECT DISTINCT merge_genes.sfeat , merge_genes.left_gene , \
                       %(syn_map1)s.qfeat as left_gene_qfeat FROM merge_genes, %(syn_map1)s WHERE merge_genes.left_gene \
                        = %(syn_map1)s.sfeat) as left_genes , %(bed_table_name)s \
                        WHERE left_genes.left_gene_qfeat = %(bed_table_name)s.accn)" %params
    cursor.execute(left_region_new)
    # 
    # right_region = "CREATE TABLE IF NOT EXISTS right_region AS \
    #                 (SELECT right_genes.sfeat, right_genes.right_homolog, \
    #                 %(bed_table_name)s.start as Rstart, %(bed_table_name)s.end as Rend, \
    #                 %(bed_table_name)s.chr, %(bed_table_name)s.strand \
    #                 FROM right_genes, %(bed_table_name)s \
    #                 WHERE right_genes.right_homolog = %(bed_table_name)s.accn)" %params
    #cursor.execute(right_region)
    right_region_new = "CREATE TABLE right_region as(SELECT DISTINCT right_genes.* , %(bed_table_name)s.start , %(bed_table_name)s.end , %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                      FROM (SELECT DISTINCT merge_genes.sfeat , merge_genes.right_gene , %(syn_map1)s.qfeat as right_gene_qfeat\
                      FROM merge_genes, %(syn_map1)s \
                      WHERE merge_genes.right_gene = %(syn_map1)s.sfeat) as right_genes , %(bed_table_name)s \
                      WHERE right_genes.right_gene_qfeat = %(bed_table_name)s.accn)" %params
                      
    cursor.execute(right_region_new)
                      
    # region = "CREATE TABLE region AS(SELECT  left_region.sfeat, left_region.Lend as start,\
    #              right_region.Rstart as end , right_region.chr as seqid, right_region.strand \
    #              FROM left_region , right_region \
    #              WHERE left_region.sfeat = right_region.sfeat \
    #              AND right_region.chr = left_region.chr AND right_region.strand = left_region.strand)"
    region = "CREATE TABLE Region as(SELECT left_region.sfeat , left_region.left_gene, left_region.left_gene_qfeat , left_region.start as Lstart, left_region.end as Lend, \
                                     right_region.right_gene, right_region.right_gene_qfeat, right_region.start as Rstart, right_region.end as Rend, right_region.chr as seqid, right_region.strand\
                                     FROM left_region, right_region\
                                     WHERE left_region.sfeat = right_region.sfeat AND left_region.strand = right_region.strand AND left_region.chr = right_region.chr)"
    cursor.execute(region)
    
    add_accn_to_table = "ALTER TABLE Region \
                         ADD accn INTEGER AUTO_INCREMENT NOT NULL FIRST, \
                         ADD PRIMARY KEY(accn)"
    cursor.execute(add_accn_to_table)

def happy_happy_joy_joy(cursor):
    ""
    same_strand = "CREATE TABLE same_strand as(SELECT Region.accn, Region.sfeat, Region.Right_gene ,  Region.left_gene, Region.Lend as start, region.Rstart as end , \
                                              (region.Rstart - Region.Lend ) as diff,  Region.seqid , Region.strand \
                                               FROM Region \
                                               WHERE (region.Rstart - Region.Lend ) > 0)"
    cursor.execute(same_strand)
                                
    opp_strand = "CREATE TABLE opp_strand as(SELECT Region.accn, Region.sfeat, Region.Right_gene ,  Region.left_gene, Region.Rend as start, region.Lstart as end , \
                                            (region.Lstart - Region.Rend ) as diff,  Region.seqid , Region.strand \
                                            FROM Region \
                                            WHERE (region.Rstart - Region.Lend ) < 0)"
    cursor.execute(opp_strand)

######################### LOAD DATA TO MYSQL #######################################
def connect_mysql(db_name):
    db=MySQLdb.connect(host="127.0.0.1", user="root", db= db_name)
    cursor=db.cursor()
    return cursor

def import_bed_to_mysql(bed, qfeat, sfeat, cursor, bed_table_name):
    for gene in bed:
        if gene[3] == qfeat or gene[3] == sfeat:
            insert_statement = "REPLACE INTO %s VALUES('%s', %d, %d,'%s', '%s')" \
                                 % (bed_table_name, gene[3],int(gene[1]),int(gene[2]),\
                                 gene[0],gene[5])
            cursor.execute(insert_statement)

def import_pairs_to_mysql(qfeat, sfeat, table_name, cursor):
    "puts qfeat and sfeat into mysql"                            
    insert_statement = "INSERT INTO %s VALUES('%s', '%s')" %(table_name, qfeat, sfeat) 
    cursor.execute(insert_statement)

def grab_pairs(dagchain_file):
    "grabs pairs and then runs mysql" 
    pairs_list = []
    for line in open(dagchain_file, 'r'):
        pairs = line.strip().split("\t")
        if len(pairs) == 2:
            pair = pairs[0], pairs[1]
            pairs_list.append(pair)
    return pairs_list

def create_mysql_tables(table_name, bed_table_name, dagfile, bed, cursor):
    syn_table_stmt = "CREATE TABLE IF NOT EXISTS %s (sfeat VARCHAR(20),\
                     qfeat VARCHAR(20), UNIQUE (sfeat, qfeat))" % table_name
    cursor.execute(syn_table_stmt)
    bed_table_stmt = "CREATE TABLE IF NOT EXISTS %s (accn VARCHAR(20), start INTEGER,\
                    end INTEGER, Chr VARCHAR(2), Strand VARCHAR(2))" % bed_table_name
    cursor.execute(bed_table_stmt)
    pairs_list = grab_pairs(dagfile)
    for pairs in pairs_list:
        qfeat, sfeat = pairs
        import_bed_to_mysql(bed, qfeat, sfeat, cursor, bed_table_name)
        import_pairs_to_mysql(qfeat, sfeat, table_name, cursor)

######################### MYSQL #######################################
def main(syn_map1, syn_map2, bed_table_name, dagfile, dagfile2, bed, db_name):
    """main call for importing into mysql"""
    cursor = connect_mysql(db_name)
    # create_mysql_tables(syn_map1, bed_table_name, dagfile, bed, cursor)
    # create_mysql_tables(syn_map2, bed_table_name, dagfile2, bed, cursor)
    # remove_retined_homologs(syn_map1, syn_map2, bed_table_name, cursor)
    # print("Removed retined homologs")
    # grab_genes_to_left(syn_map1, bed_table_name, cursor)
    # print("Removed retined homologs")
    # grab_genes_to_right(syn_map1, bed_table_name, cursor)
    # print("Removed retined homologs")
    # merge_left_right_gene_tables(syn_map1, bed_table_name, cursor)
    create_region_table(bed_table_name, syn_map1, cursor)
    # print("Removed retined homologs")
    happy_happy_joy_joy(cursor)

bed = Bed('/Users/gturco/data/rice_v6.bed')
dagchain_file = ('/Users/gturco/data/rice_v6_rice_v6.pairs.txt')
dagchain_file2 = ('/Users/gturco/data/rice_v6_sorghum_v1.pairs.txt')
main('rice_rice', 'rice_sorg', 'rice_bed', dagchain_file, dagchain_file2, bed, 'find_region2')


#right_gene = grab_genes_to_right('syn_map6', 'rice_bed', cursor)
#remove_retined_homologs('syn_map6', 'syn_map_sorg', 'rice_bed', cursor)
#start_postions = [22366890, 22376407, 22382443, 22399588, 22426984, 22507403, 22513025, 22526772,
#22536132, 22560739, 22565971, 22598493, 22638131, 22648606, 22653368, 22660244, 22665825]
#for site in start_postions:
#    left_gene_list =[]
#    left_gene = gene_to_left(site, 'syn_map6', 'rice_bed')
#    left_gene_list.append(left_gene)
#    print left_gene_list

####################### match chrm number and strand still need to fix ... strand####################