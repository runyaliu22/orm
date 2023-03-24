
from sqlalchemy import create_engine, text



# connect to PostgreSQL database on virtual machine with IP address 192.168.0.100
engine = create_engine('postgresql://postgres:passw0rd@localhost:5432/ACC_BBALL')
#vcm-30729.vm.duke.edu

conn = engine.connect()


def query1(conn, use_mpg, min_mpg, max_mpg,
           use_ppg, min_ppg, max_ppg,
           use_rpg, min_rpg, max_rpg,
           use_apg, min_apg, max_apg, 
           use_spg, min_spg, max_spg, 
           use_bpg, min_bpg, max_bpg):

    conditions = []
    if use_mpg:
        conditions.append("mpg >= :min_mpg AND mpg <= :max_mpg")
    if use_ppg:
        conditions.append("ppg >= :min_ppg AND ppg <= :max_ppg")
    if use_rpg:
        conditions.append("rpg >= :min_rpg AND rpg <= :max_rpg")
    if use_apg:
        conditions.append("apg >= :min_apg AND apg <= :max_apg")
    if use_spg:
        conditions.append("spg >= :min_spg AND spg <= :max_spg")
    if use_bpg:
        conditions.append("bpg >= :min_bpg AND bpg <= :max_bpg")

    stmt = text('SELECT * FROM player WHERE ' + ' AND '.join(conditions))

    result = conn.execute(stmt, {'min_mpg': min_mpg, 'max_mpg': max_mpg,
                                 'min_ppg': min_ppg, 'max_ppg': max_ppg,
                                 'min_rpg': min_rpg, 'max_rpg': max_rpg,
                                 'min_apg': min_apg, 'max_apg': max_apg,
                                 'min_spg': min_spg, 'max_spg': max_spg,
                                 'min_bpg': min_bpg, 'max_bpg': max_bpg})

    rows = result.fetchall()

    # fetch the results and print them
    print("PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG")
    for row in rows:
        print(f"{row[0]} {row[1]} {row[2]} {row[3]} {row[4]} {row[5]} {row[6]} {row[7]} {row[8]} {row[9]} {row[10]}")

    
    # fetch the results and print them
    #for row in result:
    
    #print(f"{row[0]} {row[1]} {row[2]} {row[3]} {row[4]} {row[5]} {row[6]} {row[7]} {row[8]} {row[9]} {row[10]}" for row in rows)



#query1(conn, 1, 35, 40, 1, 15, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)






def query2(conn, color):
    # create an SQLAlchemy text object that represents the SQL statement
    stmt = text('SELECT t.name FROM team t '
                'JOIN color c ON c.color_id = t.color_id '
                'WHERE c.name = :color')

    # execute the SQL statement using the engine object

    result = conn.execute(stmt, {'color': color})#placeholder!

    # fetch the results and print them
    teams = [row[0] for row in result]
    

    #print(f'Teams in {color} \n{", ".join(teams)}')# '/" f means placeholder

    print("NAME")
    for name in teams:
        print(name)



#query2(conn, 'Red')

def query3(conn, team_name):
    # construct the SQL query
    stmt = text('SELECT p.first_name, p.last_name '
                'FROM player p '
                'JOIN team t ON p.team_id = t.team_id '
                'WHERE t.name = :team_name '
                'ORDER BY p.ppg DESC')

    # execute the query and fetch the results
    result = conn.execute(stmt, {'team_name': team_name})
    rows = result.fetchall()

    # extract the player names and print them
    player_names = [f'{row[0]} {row[1]}' for row in rows]
    
    print("FIRST_NAME LAST_NAME")
    for name in player_names:
        print(name)

#query3(conn, "BostonCollege")
   
def query4(conn, state_name, color_name):
    # construct the SQL query
    stmt = text('SELECT p.uniform_num, p.first_name, p.last_name '
                'FROM player p '
                'JOIN team t ON p.team_id = t.team_id '
                'JOIN color c ON t.color_id = c.color_id '
                'JOIN state s ON t.state_id = s.state_id '
                'WHERE s.name = :state_name AND c.name = :color_name')

    # execute the query and fetch the results
    result = conn.execute(stmt, {'state_name': state_name, 'color_name': color_name})
    rows = result.fetchall()

    # extract the player information and print it
    player_info = [f'{row[0]} {row[1]} {row[2]}' for row in rows]
    
    print("UNIFORM_NUM FIRST_NAME LAST_NAME")
    for info in player_info:
        print(info)


#query4(conn, "NC", "Red");

def query5(conn, num_wins):
    # construct the SQL query
    stmt = text('SELECT p.first_name, p.last_name, t.name, t.wins '
                'FROM player p '
                'JOIN team t ON p.team_id = t.team_id '
                'WHERE t.wins > :num_wins')

    # execute the query and fetch the results
    result = conn.execute(stmt, {'num_wins': num_wins})
    rows = result.fetchall()

    # extract the player and team information and print it
    info = [f'{row[0]} {row[1]} {row[2]} {row[3]}' for row in rows]
    

    print("FIRST_NAME LAST_NAME NAME WINS")

    for i in range(len(rows)):
        print(f'{info[i]}')

#query5(conn, 10)




# test the connection by executing a query
#stmt = text('SELECT * FROM player')
#result = conn.execute(stmt)

#for row in result:
#    print(row)


