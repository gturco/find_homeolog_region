from syn_map_import import mysql_syn_import
import MySQLdb
from flatfeature import Bed
import MySQLdb.cursors

def remove_retined_homologs(org1_org1_bed_table, org1_org2_bed_table, cursor): 
    "Creates a new table containg only genes that are not retained between both speices \
    input:  the bed tables for all specices\
    otuput: the genes from the bed tables that are not shared"   
    params = {'org1_org1_bed':org1_org1_bed_table, 'org1_org2_bed': org1_org2_bed_table}  
    grab_unique_genes = "CREATE TABLE IF NOT EXISTS non_retained AS \
                         (SELECT %(org1_org2_bed)s.* \
                         FROM %(org1_org2_bed)s \
                         WHERE %(org1_org2_bed)s.accn \
                         NOT IN (SELECT %(org1_org1_bed)s.accn FROM %(org1_org1_bed)s, %(org1_org2_bed)s \
                         WHERE %(org1_org2_bed)s.accn = %(org1_org1_bed)s.accn))" % params
    cursor.execute(grab_unique_genes)


def insert_gene(sfeat, gene, table_name , cursor):
    "inserts bed infromation for the nearest homelog to the left/right into either the left or right table \
    also inserts the orginal gene of intrest"
    insert_genes = "INSERT INTO %s (sfeat, accn, start, end, chr, strand) \
                      VALUES ('%s','%s',%d, %d,'%s','%s')"  %(table_name, sfeat['accn'], gene['accn'], gene['start'], gene['end'], gene['Chr'], gene['Strand'])
    cursor.execute(insert_genes)

def grab_gene_to_right(sfeat, bed_table, cursor):
    "grabs the nerest homelog to the right of the gene of intrest (on either strand) \
    input: gene of intrest, bed table \
    output: the homelog closet to the right of the gene of intrest (format: gene name and bed table information)"
    sfeat['bed_table'] = bed_table
    # Chr and Strand need to be caps to work 
    right_gene_bed = "SELECT %(bed_table)s.* \
                       FROM %(bed_table)s \
                       WHERE %(bed_table)s.chr = '%(Chr)s' AND \
                       %(bed_table)s.start > %(end)d \
                       ORDER BY %(bed_table)s.start ASC LIMIT 1" %(sfeat)
    print right_gene_bed
    cursor.execute(right_gene_bed)
    right_gene = cursor.fetchone()
    return right_gene

def grab_gene_to_left(sfeat, bed_table, cursor):
    "grabs the nerest homelog to the left of the gene of intrest (on either strand) \
    input: gene of intrest, bed table \
    output: the homelog closet to the left of the gene of intrest (format: gene name)"
    sfeat['bed_table'] = bed_table
    left_gene_bed = "SELECT %(bed_table)s.* \
                       FROM %(bed_table)s \
                       WHERE %(bed_table)s.chr = '%(Chr)s' AND \
                       %(bed_table)s.end < %(start)d \
                       ORDER BY %(bed_table)s.start DESC LIMIT 1" %(sfeat)
    print left_gene_bed
    cursor.execute(left_gene_bed)
    left_gene = cursor.fetchone()
    return left_gene

def merge_tables(cursor):
    "merges the left and right table based on the sfeat"
    merge = "CREATE TABLE merge_genes AS(SELECT DISTINCT left_genes.sfeat, left_genes.accn as left_gene, right_genes.accn as right_gene,\
            left_genes.start as left_start, left_genes.end as left_end, right_genes.start as right_start, \
            right_genes.end as right_end, (right_genes.start - left_genes.end ) as sdiff, right_genes.chr as chr, left_genes.strand as lstrand ,right_genes.strand as rstrand \
            FROM left_genes , right_genes \
            WHERE left_genes.sfeat = right_genes.sfeat AND left_genes.chr = right_genes.chr)"
    cursor.execute(merge)
    
    add_accn_to_table = "ALTER TABLE merge_genes \
                         ADD accn INTEGER AUTO_INCREMENT NOT NULL FIRST, \
                         ADD PRIMARY KEY(accn)"
    cursor.execute(add_accn_to_table)

def find_left_right_gene (bed_table, cursor):
    "finds the closet homelog to the left and the right of the gene of intrest \
    imput: bed information for the non_retained gene of intrest \
    output: the homelog to the left and right with its bed info"
 #   filter_bed_table(syn_map1 , bed_table_name , cursor)
    select_non_retained_genes = "SELECT * FROM non_retained"
    cursor.execute(select_non_retained_genes)
    #creates a dictionary for the non_retained_genes table
    non_retained = cursor.fetchall()
    for sfeat in non_retained:
        right_gene = grab_gene_to_right(sfeat, bed_table, cursor)
        left_gene = grab_gene_to_left(sfeat, bed_table, cursor)
        if left_gene is None:
            print "left_gene:" ,sfeat['accn']
        else:
            insert_gene(sfeat, left_gene, 'left_genes', cursor)
        if right_gene is None:
            print "right_gene :" , sfeat['accn']
        else:
            insert_gene(sfeat, right_gene , 'right_genes', cursor)
    merge_tables(cursor)

def create_left_right_tables(name ,cursor):
    "creates the table that will be used to insert genes to the left and right of the gene of intrest \
    imput : name of the table,  output: table "
    stmt = "CREATE TABLE IF NOT EXISTS %s (sfeat Varchar(30), accn Varchar(30), \
            start INTEGER, end INTEGER , chr Varchar(30), strand Varchar(30))" % name
    cursor.execute(stmt)



def create_region_table(bed_table_name , syn_map1, cursor):
    """Creates a table that contains the homelog of the left and right gene along with its bed informations\
    currently only grabs homelogs that remain on the same strand to make guessing the sfeat strand easier \
    CHANGE here NEXT grab all qfeats independt of strand then filter for if they both change or both stay the same"""
    params = {'bed_table_name': bed_table_name, 'syn_map1' : syn_map1}
    left_region_new = "CREATE TABLE left_region as(SELECT DISTINCT left_genes.* , %(bed_table_name)s.start , %(bed_table_name)s.end , %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                       FROM (SELECT DISTINCT merge_genes.accn, merge_genes.sfeat , merge_genes.left_gene, merge_genes.lstrand,\
                       %(syn_map1)s.qfeat as left_gene_qfeat FROM merge_genes, %(syn_map1)s WHERE merge_genes.left_gene \
                       = %(syn_map1)s.sfeat) as left_genes , %(bed_table_name)s \
                       WHERE left_genes.left_gene_qfeat = %(bed_table_name)s.accn AND left_genes.lstrand = %(bed_table_name)s.strand)" %params
    print left_region_new                   
    cursor.execute(left_region_new)
    
    right_region_new = "CREATE TABLE right_region as(SELECT DISTINCT right_genes.* , %(bed_table_name)s.start , %(bed_table_name)s.end , %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                      FROM (SELECT DISTINCT merge_genes.accn, merge_genes.sfeat , merge_genes.right_gene, merge_genes.rstrand , %(syn_map1)s.qfeat as right_gene_qfeat\
                      FROM merge_genes, %(syn_map1)s \
                      WHERE merge_genes.right_gene = %(syn_map1)s.sfeat) as right_genes , %(bed_table_name)s \
                      WHERE right_genes.right_gene_qfeat = %(bed_table_name)s.accn AND right_genes.rstrand = %(bed_table_name)s.strand)" %params
    cursor.execute(right_region_new)
    
    region = "CREATE TABLE Region as(SELECT left_region.accn, left_region.sfeat , left_region.left_gene, left_region.left_gene_qfeat , left_region.start as Lstart, left_region.end as Lend, \
                                     right_region.right_gene, right_region.right_gene_qfeat, right_region.start as Rstart, right_region.end as Rend, right_region.chr as seqid, right_region.strand as rstrand, left_region.strand as lstrand\
                                     FROM left_region, right_region\
                                     WHERE left_region.sfeat = right_region.sfeat AND left_region.chr = right_region.chr)"
    print region
    cursor.execute(region)

def remove_garbage(cursor):
    "finds results where the perdicted region is negtive \
    due to transpo... deletions ect \
    imputs: regions size, output: two table one with negtive regions one with postive(shoulnt be neg cause selecting qfeat for same strand)  "
    
    pos_diff = "CREATE TABLE postive_diff as(SELECT Region.accn, Region.sfeat, Region.Right_gene , Region.Right_gene_qfeat,  Region.left_gene, Region.left_gene_qfeat, Region.Lend as start, region.Rstart as end , \
                                              (region.Rstart - Region.Lend ) as diff,  Region.seqid  \
                                               FROM Region \
                                               WHERE (region.Rstart > Region.Lend ))"
    cursor.execute(pos_diff)
    
    neg_diff = "CREATE TABLE neg_diff as(SELECT Region.accn, Region.sfeat, Region.Right_gene , Region.Right_gene_qfeat, Region.left_gene, Region.left_gene_qfeat, Region.Rend as start, region.Lstart as end , \
                                            (region.Lend - Region.Rstart) as diff,  Region.seqid \
                                            FROM Region \
                                            WHERE (region.Rstart < Region.Lend ))"
    cursor.execute(neg_diff)

def create_final_table(org1_org2, cursor):
    """creates the final table outline, then inserts regions under 100000 bps from both the opp and same strand """
    
    create_table = "CREATE TABLE IF NOT EXISTS FINAL (accn INTEGER , sfeat Varchar(1000), sleft_gene Varchar(1000),\
                    sright_gene Varchar(1000), sstart INTEGER , send INTEGER, sdiff INTEGER, qleft_gene Varchar(1000), qright_gene Varchar(1000), \
                    start INTEGER, end INTEGER, diff INTEGER, seqid Varchar(10), strand Varchar(100), url Varchar(10000))"
    cursor.execute(create_table)
    
    small_regions_pos ="INSERT INTO FINAL (accn , sfeat, sleft_gene, sright_gene ,qleft_gene, qright_gene , start , end , diff, seqid) \
                        SELECT postive_diff.accn, postive_diff.sfeat, postive_diff.left_gene, postive_diff.right_gene, \
                        postive_diff.left_gene_qfeat, postive_diff.right_gene_qfeat, postive_diff.start, postive_diff.end, postive_diff.diff, \
                        postive_diff.seqid FROM postive_diff WHERE postive_diff.diff < 100000"
    cursor.execute(small_regions_pos)
    # code for putting in negtive regions to final
    # small_regions_opp = "INSERT INTO FINAL (accn , sfeat, sleft_gene, sright_gene , qleft_gene, qright_gene , start , end , diff, seqid, orientation) \
    #                     SELECT opp_strand.accn, opp_strand.sfeat, opp_strand.left_gene, opp_strand.right_gene, \
    #                     opp_strand.left_gene_qfeat ,opp_strand.right_gene_qfeat, opp_strand.start, opp_strand.end, opp_strand.diff, \
    #                      opp_strand.seqid, 'opp' FROM opp_strand WHERE opp_strand.diff < 100000"
    # cursor.execute(small_regions_opp)
    
    sgenes_info = "UPDATE FINAL, merge_genes SET FINAL.sdiff = merge_genes.sdiff, sstart = merge_genes.left_end , send =  merge_genes.right_start \
                   WHERE merge_genes.sfeat = FINAL.sfeat"
    cursor.execute(sgenes_info)
    
    url('FINAL', org1_org2, cursor)
    cursor.execute("SELECT * FROM  left_genes")
    left_genes = cursor.fetchall()
    for left_gene in left_genes:
        print left_gene
        assign_strand(left_gene, cursor)

def url(tablename, syn_map2, cursor, base ="http://synteny.cnr.berkeley.edu/CoGe/GEvo.pl?"):
    "creates url for each sfeat, matching the homelogous regions and inserts into mysql"
    cursor.execute("SELECT * FROM  %s " %tablename)
    region_dict = cursor.fetchall()
    for d in region_dict:
        # if d['orientation'] == 'same':
        sfeat = d['sfeat']
        accn1 = d['qleft_gene'] 
        d['diff'] += 15000
        accn2 = d['sleft_gene'] 
        d['sdiff'] += 15000
        d['qfeat'] = grab_qfeat(sfeat, syn_map2, cursor)
        url = "drup1=10000&drdown1=%(diff)s&drup2=10000&drdown2=%(sdiff)s;accn1=%(qleft_gene)s;accn2=%(sleft_gene)s;accn3=%(qfeat)s;dr3up=10000;dr3down=10000;num_seqs=3"
        d['url'] = base + url % d
        import_url_to_mysql(tablename , d['url'], sfeat, cursor)
        # elif d['orientation'] == 'opp': 
        #     sfeat = d['sfeat']
        #     accn1 = d['qleft_gene'] 
        #     d['diff'] += 10000
        #     accn2 = d['sleft_gene'] 
        #     d['sdiff'] += 10000
        #     d['qfeat'] = grab_qfeat(sfeat, syn_map2, cursor)
        #     url = "drup1=%(diff)s&drdown1=1000&drup2=1000&drdown2=%(sdiff)s;accn1=%(qleft_gene)s;accn2=%(sleft_gene)s;accn3=%(qfeat)s;dr3up=10000;dr3down=10000;num_seqs=3"
        #     d['url'] = base + url % d
        #     import_url_to_mysql(tablename , d['url'], sfeat, cursor)

def grab_qfeat(accn, syn_map2, cursor):
    "grabs the sorg qfeat for the third panell"
    params = {'accn' : accn , 'syn_map2': syn_map2}
    stmt = "SELECT %(syn_map2)s.qfeat \
            FROM %(syn_map2)s \
            WHERE '%(accn)s' = %(syn_map2)s.sfeat" %params
            
    print stmt
    cursor.execute(stmt) 
    qfeat_dict = cursor.fetchone()
    return qfeat_dict['qfeat']

def import_url_to_mysql(tablename, url, sfeat, cursor):
    stmt = "UPDATE {0} SET url = '{1}' WHERE sfeat = '{2}'".format(tablename, url, sfeat)
    print stmt
    cursor.execute(stmt)

def assign_strand(left_gene, cursor):
    "assigns a strand to the regions based on the sfeat and left_gene orienations \
    the dictionary uses numbers based on column number"
    stmt =   "SELECT  non_retained.* FROM non_retained\
             WHERE '%s' = non_retained.accn" %left_gene['sfeat']
    cursor.execute(stmt)
    sfeat = cursor.fetchone()
    if left_gene['strand'] == sfeat['Strand']:
        update_strand(sfeat['Strand'], sfeat, left_gene, cursor)
    else:
        if left_gene['strand'] == '+':
            update_strand('-', sfeat, left_gene, cursor)
        else:
            update_strand('+', sfeat, left_gene, cursor)

def update_strand(sign, sfeat, left_gene, cursor):
    stmt = "UPDATE FINAL SET FINAL.strand = '{0}'\
                   WHERE FINAL.sfeat = '{1}' AND FINAL.sleft_gene = '{2}' ".format(sign, sfeat['accn'], left_gene['accn'])
    cursor.execute(stmt)


def main(db_name, org1_org1, org1_org1_path, org1_org2, org1_org2_path, bed_path):
    """runs main function"""
    
    db=MySQLdb.connect(host="127.0.0.1", user="root", db= db_name)
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    
    synmap1 = mysql_syn_import(db_name, org1_org1)
    synmap1.org1_org1(org1_org1_path)
    synmap1.import_bed_to_mysql(bed_path , org1_org1_path)
    synmap2 = mysql_syn_import(db_name, org1_org2)
    synmap2.org1_org2(org1_org2_path)
    synmap2.import_bed_to_mysql(bed_path , org1_org2_path)
    create_left_right_tables('right_genes', cursor)
    create_left_right_tables('left_genes', cursor)
    org1_org2_bed_table = '%s_bed' %org1_org2 
    org1_org1_bed_table = '%s_bed' %org1_org1
    remove_retined_homologs(org1_org1_bed_table, org1_org2_bed_table, cursor)
    find_left_right_gene(org1_org1_bed_table, cursor)
    create_region_table(org1_org1_bed_table , org1_org1, cursor)
    remove_garbage(cursor)
    create_final_table(org1_org2, cursor)


    

main('find_homeo_2', 'rice_rice', '/Users/gturco/results/data/rice_v6_rice_v6.pairs.txt', 'rice_sorg', '/Users/gturco/results/data/rice_v6_sorghum_v1.pairs.txt', '/Users/gturco/results/data/rice_v6.bed')