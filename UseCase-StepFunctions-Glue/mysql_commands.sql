
mysql -h transactionaldb.ctnto3tke9wq.eu-west-1.rds.amazonaws.com -P 3306 -u admin -p

CREATE TABLE stackoverflow_posts (
  id INT,
  title VARCHAR(255),
  body TEXT,
  accepted_answer_id INT,
  answer_count INT,
  comment_count INT,
  community_owned_date DATETIME,
  creation_date DATETIME,
  favorite_count INT,
  last_activity_date DATETIME,
  last_edit_date DATETIME,
  last_editor_display_name VARCHAR(255),
  last_editor_user_id INT,
  owner_display_name VARCHAR(255),
  owner_user_id INT,
  parent_id INT,
  post_type_id INT,
  score INT,
  tags VARCHAR(255),
  view_count INT
);

LOAD DATA LOCAL INFILE "stackoverflowposts.csv" INTO TABLE stackoverflow_posts FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
