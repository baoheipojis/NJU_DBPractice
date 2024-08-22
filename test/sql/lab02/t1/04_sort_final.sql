open database db2024;
select * from dbcourse order by id;
select * from dbcourse order by l2_score;
select * from dbcourse order by desc l2_score;
select * from dbcourse order by age;
select * from dbcourse order by desc age;
select * from dbcourse order by address;
select * from dbcourse order by desc address;
select * from dbcourse order by name, address;
select * from dbcourse order by desc name, address;
select * from dbcourse order by l2_score, address;
select * from dbcourse order by desc l2_score, address;
select age, name, id from dbcourse order by age, name;
select age, name, id from dbcourse order by desc age, name;

--- complex query with all limit, order by, where, projection
select age, name, id from dbcourse where age > 20 order by age, name limit 10;
select * from dbcourse where age > 20 order by desc age, name limit 10;
select gpa, name, address from dbcourse where age > 20 order by age, name limit 10;
select age, name, id from dbcourse where age > 20 and id <> age order by desc age, name limit 10;
exit;