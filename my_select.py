from pprint import pprint
from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_1():
    """
    1. Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    SELECT s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    :return:
    """
    result = session.query(
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Student)\
        .group_by(Student.id)\
        .order_by(desc('avg_grade'))\
        .limit(5).all()
    # order_by(Grade.grade.desc())
    return result


def select_2():
    """
     2. Найти студента с наивысшим средним баллом по определенному предмету.
    SELECT d.name, s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.id = 2
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 1;
    :return:
    """
    result = session.query(
        Discipline.name,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).join(Discipline)\
        .filter(Discipline.id == 2)\
        .group_by(Student.id, Discipline.name)\
        .order_by(desc('avg_grade')).limit(1).first()
    return result


def select_3():
    """
        -- 3. Найти средний балл в группах по определенному предмету.

        SELECT gr.name as group, d.name as discipline, ROUND(AVG(g.grade), 2) as avg_grade
        FROM grades g
        LEFT JOIN students s ON s.id = g.student_id
        LEFT JOIN disciplines d ON d.id = g.discipline_id
        LEFT JOIN "groups" gr  ON gr.id = s.group_id 
        WHERE d.id = 6
        GROUP BY gr.id, d.id 
        ORDER BY avg_grade DESC;

    """
    result = session.query(
        Group.name,
        Discipline.name,
        func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
        .select_from(Grade).join(Student).join(Discipline).join(Group)\
        .filter(Discipline.id == 6).group_by(Group.id, Discipline.id)\
        .order_by(desc("avg_grade")).all()
    return result


def select_4():
    """
        -- 4. Найти средний балл на потоке (по всей таблице оценок).

        SELECT ROUND(AVG(g.grade), 2) as avg_grade
        FROM grades g 

    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
        .select_from(Grade).first()
    return result


def select_5():
    """
        -- 5. Найти какие курсы читает определенный преподаватель.

        select t.fullname, d.name 
        from teachers t  
        left join disciplines d on t.id = d.teacher_id 
        WHERE t.id = 3

    """
    result = session.query(Teacher.fullname, Discipline.name)\
        .select_from(Teacher).join(Discipline).filter(Teacher.id == 2).all()
    return result


def select_6():
    """
        -- 6. Найти список студентов в определенной группе.

        SELECT g.name as "group", s.fullname as student
        from  students s 
        join "groups" g on g.id = s.group_id 
        WHERE g.id = 1
        ORDER BY s.fullname

    """
    result = session.query(Group.name, Student.fullname)\
        .select_from(Student).join(Group)\
        .filter(Group.id == 1).order_by(Student.fullname).all()
    return result


def select_7():
    """
        -- 7. Найти оценки студентов в отдельной группе по определенному предмету.

        SELECT gr.name as group_ , d.name as discipline , s.fullname as student , g.grade 
        from students s 
        join "groups" gr on gr.id = s.group_id 
        JOIN grades g on s.id = g.student_id 
        JOIN disciplines d on d.id = g.discipline_id 
        WHERE gr.id = 1 AND d.id = 1
        ORDER BY s.id 

    """
    result = session.query(
        Group.name, Discipline.name, Student.fullname, Grade.grade)\
        .select_from(Student).join(Group).join(Grade).join(Discipline)\
        .filter(Group.id == 1).filter(Discipline.id == 1)\
        .order_by(Student.id).all()
    return result


def select_8():
    """
            -- 8. Найти средний балл, который ставит определенный преподаватель по своим предметам.

            SELECT t.fullname as teachar, d.name as discipline, round(AVG(g.grade), 2) as avg_grade
            from teachers t 
            left join disciplines d on t.id  = d.teacher_id 
            left JOIN grades g  on d.id = g.discipline_id 
            WHERE t.id  = 3
            GROUP BY d.id, t.id 

    """
    result = session.query(
        Teacher.fullname,
        Discipline.name,
        func.round(func.avg(Grade.grade), 2))\
        .select_from(Teacher)\
        .join(Discipline).join(Grade)\
        .filter(Teacher.id == 3)\
        .group_by(Discipline.id, Teacher.id).first()
    return result


def select_9():
    """
        -- 9. Найти список курсов, которые посещает определенный студент.

        SELECT s.fullname as student, d.name as discipline
        from students s 
        left join grades g  on s.id = g.student_id 
        LEFT JOIN disciplines d on d.id = g.discipline_id 
        WHERE s.id = 10
        GROUP BY d.id , s.id 

    """
    result = session.query(Student.fullname, Discipline.name)\
        .select_from(Student).join(Grade).join(Discipline)\
        .filter(Student.id == 10)\
        .group_by(Discipline.id, Student.id).all()
    return result


def select_10():
    """
        -- 10. Список курсов, которые определенному студенту читает определенный преподаватель.

        SELECT d.name as discipline, s.fullname as student, t.fullname as teachar
        from disciplines d 
        join teachers t on t.id  = d.teacher_id 
        JOIN grades g on d.id = g.discipline_id 
        JOIN students s  on s.id = g.student_id 
        WHERE s.id = 10 AND t.id = 5
        GROUP BY d.id , s.id , t.id 

    """
    result = session.query(
        Discipline.name, Student.fullname, Teacher.fullname)\
        .select_from(Discipline)\
        .join(Teacher).join(Grade).join(Student)\
        .filter(Student.id == 10, Teacher.id == 5)\
        .group_by(Discipline.id, Student.id, Teacher.id).all()
    return result


def select_11():
    """
        -- 11. Средний балл, который определенный преподаватель ставит определенному студенту.

        SELECT t.fullname as teacher, s.fullname as student, ROUND(AVG(g.grade), 2) as avg_grade
        FROM grades g
        join disciplines d on d.id = g.discipline_id 
        JOIN teachers t on t.id = d.teacher_id 
        JOIN students s on s.id = g.student_id 
        WHERE t.id = 3 AND s.id = 10
        group by t.id , s.id 

    """
    result = session.query(
        Teacher.fullname,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2))\
        .select_from(Grade)\
        .join(Discipline).join(Teacher).join(Student)\
        .filter(Teacher.id == 3).filter(Student.id == 10)\
        .group_by(Teacher.id, Student.id).all()
    return result


def select_12():
    """
    -- Оцінки студентів у певній групі з певного предмета на останньому занятті.
    select s.id, s.fullname, g.grade, g.date_of
    from grades g
    join students s on s.id = g.student_id
    where g.discipline_id = 3 and s.group_id = 3 and g.date_of = (
        select max(date_of)
        from grades g2
        join students s2 on s2.id = g2.student_id
        where g2.discipline_id = 3 and s2.group_id = 3
    );
    :return:
    """
    subquery = (select(func.max(Grade.date_of)).join(Student).filter(and_(
        Grade.discipline_id == 3, Student.group_id == 3
    )).scalar_subquery())

    result = session.query(
        Student.id, Student.fullname, Grade.grade, Grade.date_of)\
        .select_from(Grade)\
        .join(Student)\
        .filter(and_(
            Grade.discipline_id == 3, Student.group_id == 3, Grade.date_of == subquery
        )).all()
    return result


if __name__ == '__main__':
    # pprint(select_1())
    # pprint(select_2())
    # pprint(select_3())
    # pprint(select_4())
    # pprint(select_5())
    # pprint(select_6())
    # pprint(select_7())
    # pprint(select_8())
    # pprint(select_9())
    # pprint(select_10())
    pprint(select_11())
    # pprint(select_12())
