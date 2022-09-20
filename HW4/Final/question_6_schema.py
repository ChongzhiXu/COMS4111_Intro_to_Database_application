import mysql_check


def schema_operation_1():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
    create schema RACI
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


def schema_operation_2():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create table RACI.Person
(
	UNI char(6) not null,
	last_name varchar(128) null,
	first_name varchar(128) null,
	email varchar(128) null,
	constraint Person_pk
		primary key (UNI)
)
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res

def schema_operation_3():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create table raci.project
(
    project_id     char(8)      not null
        primary key,
    project_name   varchar(128) null,
    start_date     date         null,
    end_date       date         null,
    AccountableUNI char(6)      null,
    constraint project_person_UNI_fk
        foreign key (AccountableUNI) references raci.person (UNI)
)
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res

def schema_operation_4():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
    create table RACI.Relationship
(
	person_UNI char(6) not null,
	role enum ('Responsible', 'Consulted', 'Informed') not null,
	project_id char(8) not null,
	constraint Relationship_pk
		primary key (person_UNI, project_id),
	constraint Relationship_person_UNI_fk
		foreign key (person_UNI) references person (UNI),
	constraint Relationship_project_project_id_fk
		foreign key (project_id) references project (project_id)
)
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res

def schema_operation_5():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create trigger RACI.proju
    before update
    on relationship
    for each row
begin
    if new.person_UNI not in (select AccountableUNI from project where project_id = new.project_id) then
        signal SQLSTATE '45000'
            set MESSAGE_TEXT = 'A specific person can have at most one relationship to a project';
    end if;
end;
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res



def schema_operation_6():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create trigger RACI.proji
    before insert
    on relationship
    for each row
begin
    if new.person_UNI not in (select AccountableUNI from project where project_id = new.project_id) then
        signal SQLSTATE '45000'
            set MESSAGE_TEXT = 'A specific person can have at most one relationship to a project';
    end if;
end;
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


def schema_operation_7():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create trigger RACI.AccountableI
    before insert
    on project
    for each row
begin
    if new.AccountableUNI not in (select AccountableUNI from project) then
        signal SQLSTATE '45000'
            set MESSAGE_TEXT = 'There is exactly one person who is Accountable for a project';
    end if;
end;"""

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res

def schema_operation_8():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create trigger RACI.AccountableU
    before update
    on project
    for each row
begin
    if new.AccountableUNI not in (select AccountableUNI from project) then
        signal SQLSTATE '45000'
            set MESSAGE_TEXT = 'There is exactly one person who is Accountable for a project';
    end if;
end;"""

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


