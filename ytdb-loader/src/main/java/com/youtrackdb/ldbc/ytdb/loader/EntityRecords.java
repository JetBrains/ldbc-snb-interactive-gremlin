package com.youtrackdb.ldbc.ytdb.loader;

import java.util.List;

import static com.youtrackdb.ldbc.ytdb.loader.CsvProcessor.parseList;
import static com.youtrackdb.ldbc.ytdb.loader.CsvProcessor.parseString;

/**
 * Record types for LDBC SNB entities and relationships.
 */
public final class EntityRecords {

    private EntityRecords() {}

    // ==================== STATIC ENTITIES ====================

    public record Place(long id, String name, String url, String type) {
        public static Place parse(String[] fields) {
            return new Place(
                Long.parseLong(fields[0]),
                fields[1],
                fields[2],
                fields[3]
            );
        }
    }

    public record Organisation(long id, String type, String name, String url) {
        public static Organisation parse(String[] fields) {
            return new Organisation(
                Long.parseLong(fields[0]),
                fields[1],
                fields[2],
                fields[3]
            );
        }
    }

    public record TagClass(long id, String name, String url) {
        public static TagClass parse(String[] fields) {
            return new TagClass(
                Long.parseLong(fields[0]),
                fields[1],
                fields[2]
            );
        }
    }

    public record Tag(long id, String name, String url) {
        public static Tag parse(String[] fields) {
            return new Tag(
                Long.parseLong(fields[0]),
                fields[1],
                fields[2]
            );
        }
    }

    // ==================== DYNAMIC ENTITIES ====================

    public record Person(
        long id,
        String firstName,
        String lastName,
        String gender,
        long birthday,
        long creationDate,
        String locationIP,
        String browserUsed,
        List<String> languages,
        List<String> emails
    ) {
        public static Person parse(String[] fields) {
            return new Person(
                Long.parseLong(fields[0]),
                fields[1],
                fields[2],
                fields[3],
                Long.parseLong(fields[4]),
                Long.parseLong(fields[5]),
                fields[6],
                fields[7],
                parseList(fields[8]),
                parseList(fields[9])
            );
        }
    }

    public record Forum(long id, String title, long creationDate) {
        public static Forum parse(String[] fields) {
            return new Forum(
                Long.parseLong(fields[0]),
                fields[1],
                Long.parseLong(fields[2])
            );
        }
    }

    public record Post(
        long id,
        String imageFile,
        long creationDate,
        String locationIP,
        String browserUsed,
        String language,
        String content,
        int length
    ) {
        public static Post parse(String[] fields) {
            return new Post(
                Long.parseLong(fields[0]),
                parseString(fields[1]),
                Long.parseLong(fields[2]),
                fields[3],
                fields[4],
                fields[5],
                parseString(fields[6]),
                Integer.parseInt(fields[7])
            );
        }
    }

    public record Comment(
        long id,
        long creationDate,
        String locationIP,
        String browserUsed,
        String content,
        int length
    ) {
        public static Comment parse(String[] fields) {
            return new Comment(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                Integer.parseInt(fields[5])
            );
        }
    }

    // ==================== RELATIONSHIPS ====================

    public record SimpleEdge(long fromId, long toId) {
        public static SimpleEdge parse(String[] fields) {
            return new SimpleEdge(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1])
            );
        }
    }

    public record KnowsEdge(long person1Id, long person2Id, long creationDate) {
        public static KnowsEdge parse(String[] fields) {
            return new KnowsEdge(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                Long.parseLong(fields[2])
            );
        }
    }

    public record StudyAtEdge(long personId, long organisationId, int classYear) {
        public static StudyAtEdge parse(String[] fields) {
            return new StudyAtEdge(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                Integer.parseInt(fields[2])
            );
        }
    }

    public record WorkAtEdge(long personId, long organisationId, int workFrom) {
        public static WorkAtEdge parse(String[] fields) {
            return new WorkAtEdge(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                Integer.parseInt(fields[2])
            );
        }
    }

    public record HasMemberEdge(long forumId, long personId, long joinDate) {
        public static HasMemberEdge parse(String[] fields) {
            return new HasMemberEdge(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                Long.parseLong(fields[2])
            );
        }
    }

    public record LikesEdge(long personId, long contentId, long creationDate) {
        public static LikesEdge parse(String[] fields) {
            return new LikesEdge(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                Long.parseLong(fields[2])
            );
        }
    }
}
