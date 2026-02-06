package com.youtrackdb.ldbc.ytdb.loader;

/**
 * LDBC SNB schema constants for vertex labels, edge labels, and property names.
 */
public final class LdbcSchema {

    private LdbcSchema() {}

    // ==================== VERTEX LABELS ====================

    public static final String PERSON = "Person";
    public static final String PLACE = "Place";
    public static final String ORGANISATION = "Organisation";
    public static final String TAG_CLASS = "TagClass";
    public static final String TAG = "Tag";
    public static final String FORUM = "Forum";
    public static final String POST = "Post";
    public static final String COMMENT = "Comment";

    // ==================== EDGE LABELS ====================

    public static final String KNOWS = "KNOWS";
    public static final String IS_LOCATED_IN = "IS_LOCATED_IN";
    public static final String HAS_INTEREST = "HAS_INTEREST";
    public static final String STUDY_AT = "STUDY_AT";
    public static final String WORK_AT = "WORK_AT";
    public static final String HAS_MODERATOR = "HAS_MODERATOR";
    public static final String HAS_MEMBER = "HAS_MEMBER";
    public static final String CONTAINER_OF = "CONTAINER_OF";
    public static final String HAS_TAG = "HAS_TAG";
    public static final String HAS_CREATOR = "HAS_CREATOR";
    public static final String LIKES = "LIKES";
    public static final String REPLY_OF = "REPLY_OF";
    public static final String IS_PART_OF = "IS_PART_OF";
    public static final String IS_SUBCLASS_OF = "IS_SUBCLASS_OF";
    public static final String HAS_TYPE = "HAS_TYPE";

    // ==================== COMMON PROPERTIES ====================

    public static final String ID = "id";
    public static final String CREATION_DATE = "creationDate";

    // Person properties
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String GENDER = "gender";
    public static final String BIRTHDAY = "birthday";
    public static final String LOCATION_IP = "locationIP";
    public static final String BROWSER_USED = "browserUsed";
    public static final String LANGUAGES = "languages";
    public static final String EMAILS = "emails";

    // Place properties
    public static final String NAME = "name";
    public static final String URL = "url";
    public static final String TYPE = "type";

    // Message (Post/Comment) properties
    public static final String LANGUAGE = "language";
    public static final String CONTENT = "content";
    public static final String IMAGE_FILE = "imageFile";
    public static final String LENGTH = "length";

    // Forum properties
    public static final String TITLE = "title";

    // Edge properties
    public static final String CLASS_YEAR = "classYear";
    public static final String WORK_FROM = "workFrom";
    public static final String JOIN_DATE = "joinDate";
}
