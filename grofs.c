#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <git2.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <stddef.h>

#define FUSE_USE_VERSION 30

#include <fuse.h>

#define STR_COMMITS "commits"
#define STR_BLOBS "blobs"
#define STR_TREE "tree"
#define STR_PARENT "parent"

#define VERSION "0.1.0-alpha"

#define GIT_OBJECT_ID_LEN GIT_OID_HEXSZ

#define LOGIC_ERROR 64

// Halts in case of a logic error because it's better to exit since we don't know what else could be wrong
#define HALT(fmt, ...) fprintf(stderr, "ERROR [file: %s, line: %d]: " fmt, __FILE__, __LINE__, __VA_ARGS__); exit(LOGIC_ERROR)

enum dir_entry_type {
    NONE, LIST, ID, PATH_IN_GIT, TREE, PARENT
};

enum root_child_type {
    ROOT, COMMIT, BLOB
};

struct path_spec {
    char *buff;
    char **parts;
    int parts_count;
    enum dir_entry_type entry_type;
    enum root_child_type root_child_type;
};

enum grofs_node_type {
    DATA /* since I can't have FILE */, DIR
};

struct grofs_node {
    enum grofs_node_type type;
    enum dir_entry_type entry_type;
    enum root_child_type root_child_type;
    git_oid oid;
    time_t time;
    size_t size;
};

struct grofs_readdir_context {
    fuse_fill_dir_t filler;
    void *buffer;
    git_otype wanted_type;
};

struct grofs_file_handle {
    char *buff;
    int len;
};

struct grofs_cli_opts {
    int show_version;
    int show_help;
};

#define GROFS_STRUCT_OPT(tpl, field, value) { tpl, offsetof(struct grofs_cli_opts, field), value }

static const char *root_child_type_to_str(enum root_child_type type);
static char *path_spec_full_path(const struct path_spec *path_spec);
static const char *path_spec_blob_name(const struct path_spec *path_spec);
static const char *path_spec_blob_name(const struct path_spec *path_spec);
static char *path_spec_git_path(const struct path_spec *path_spec);
static inline int min(int a, int b);
static void free_path_spec(struct path_spec *path_spec);
static int count_char_in_string(const char *str, char needle);
static int parse_path_info_resolve_root_child(enum root_child_type *root_child_type, const char *part);
static int path_parse_commit_sub_path(struct path_spec *path_spec, int level);
static int path_parse_blob_sub_path(struct path_spec *path_spec, int level);
static char *path_spec_full_path(const struct path_spec *path_spec);
static int parse_path_init_dir_entry_type(struct path_spec *path_spec);
static int parse_path_as_root(struct path_spec **path_spec);
static int path_parse_as_root_child(struct path_spec *path_spec);
static int parse_path(struct path_spec **path_spec, const char *path);
static void cleanup_on_exit_cb();
static int fuse_args_process_cb(void *data, const char *arg, int key, struct fuse_args *out_args);
static int grofs_git_commit_parent_lookup(const git_oid *commit_oid, git_oid *parent_oid);
static int grofs_git_commit_has_parent(const git_oid *commit_oid);
static void grofs_getattr_init_stat_as_dir(struct stat *stat, time_t started_time);
static void grofs_getattr_init_stat_as_file(struct stat *stat, time_t started_time, int size);
static int grofs_resolve_node_for_path_spec_for_commit_children(struct grofs_node *node, const git_commit *commit, const struct path_spec *path_spec);
static int grofs_resolve_node_for_path_spec_for_commit_type(struct grofs_node *node, const struct path_spec *path_spec);
static int grofs_resolve_node_for_path_spec_for_blob_type(struct grofs_node *node, const struct path_spec *path_spec);
static int grofs_resolve_node_for_path_spec(struct grofs_node *node, const struct path_spec *path_spec);
static int grofs_resolve_node_for_path(struct grofs_node **node, const char *path);
static int grofs_getattr(const char *path, struct stat *stat);
static int grofs_readdir_git_collect_object_cb(const git_oid *id, void *payload);
static int grofs_readdir_from_git_tree(const git_tree *tree, void *buffer, fuse_fill_dir_t filler);
static int grofs_readdir_from_git_tree_oid(const git_oid *oid, void *buffer, fuse_fill_dir_t filler);
static int grofs_readdir_for_commit_list(void *buffer, fuse_fill_dir_t filler);
static int grofs_readdir_for_blob_list(void *buffer, fuse_fill_dir_t filler);
static int grofs_readdir_for_node(const struct grofs_node *node, void *buffer, fuse_fill_dir_t filler);
static int grofs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *file_info);
static struct grofs_file_handle *grofs_file_nandle_new(int buff_len);
static int grofs_open_node_commit_parent(const git_oid *oid, struct fuse_file_info * file_info);
static int grofs_open_node_blob(const git_oid *oid, struct fuse_file_info *file_info);
static int grofs_open_node(const struct grofs_node *node, struct fuse_file_info *file_info);
static int grofs_open(const char *path, struct fuse_file_info *file_info);
static int grofs_read(const char *path, char *buff, size_t size, off_t offset, struct fuse_file_info *file_info);
static int grofs_release(const char* path, struct fuse_file_info *file_info);
static void grofs_print_help(const char *bin_path);

static char *repo_path = NULL;
static git_repository *repo = NULL;
static time_t started_time;
struct fuse_args args = FUSE_ARGS_INIT(0, NULL);

struct fuse_operations fuse_operations = {
    .getattr	= grofs_getattr,
    .readdir	= grofs_readdir,
    .open       = grofs_open,
    .read       = grofs_read,
    .release    = grofs_release
};

static struct fuse_opt grofs_fuse_opts[] = {
    GROFS_STRUCT_OPT("-V", show_version, 1),
    GROFS_STRUCT_OPT("--version", show_version, 1),
    GROFS_STRUCT_OPT("-h", show_help, 1),
    GROFS_STRUCT_OPT("--help", show_help, 1),
    FUSE_OPT_END
};

static const char *root_child_type_to_str(enum root_child_type type) {
    switch (type) {
        case ROOT:
            return "ROOT";
        case COMMIT:
            return "COMMIT";
        case BLOB:
            return "BLOB";
        default:
            HALT("Unknown %d", type);
    }
}

static const char *grofs_node_type_to_str(enum grofs_node_type type) {
    switch (type) {
        case DIR:
            return "DIR";
        case DATA:
            return "DATA";
        default:
            HALT("Unknown %d", type);
    }
}

static const char *path_spec_commit_name(const struct path_spec *path_spec) {
    return path_spec->parts[1];
}

static const char *path_spec_blob_name(const struct path_spec *path_spec) {
    return path_spec->parts[1];
}

static char *path_spec_git_path(const struct path_spec *path_spec) {
    if (path_spec->parts_count < 4) {
        return NULL;
    }

    int count = 0;
    int i;

    for (i = 3; i < path_spec->parts_count; i++) {
        count += strlen(path_spec->parts[i]) + 1;
    }

    char *buff = (char *) malloc(sizeof(char) * count);

    if (NULL == buff) {
        return NULL;
    }

    buff[0] = '\0';

    for (i = 3; i < path_spec->parts_count; i++) {
        strcat(buff, path_spec->parts[i]);

        if (i + 1 < path_spec->parts_count) {
            strcat(buff, "/");
        }
    }

    return buff;
}

static char *path_spec_full_path(const struct path_spec *path_spec) {
    if (0 == path_spec->parts_count) {
        return "/";
    }

    char *last_part = path_spec->parts[path_spec->parts_count - 1];

    void *parts_buff_end = last_part + strlen(last_part) + 1;

    int len = (int) (parts_buff_end - (void *) path_spec->buff);

    char *buff = (char *) malloc(sizeof(char) * (1 + len));

    *buff = '/';
    memcpy(buff + 1, path_spec->buff, sizeof(char) * len);

    int i;

    for (i = 1; i < len - 1; i++) {
        if ('\0' == *(buff + 1 + i)) {
            *(buff + 1 + i) = '/';
        }
    }

    return buff;
}

static inline int min(int a, int b) {
    return a < b ? a : b;
}

static void free_path_spec(struct path_spec *path_spec) {
    free(path_spec);
}

static int count_char_in_string(const char *str, char needle) {
    int count = 0;

    for (; *str; count += needle == *str, str++);

    return count;
}

static int parse_path_info_resolve_root_child(enum root_child_type *root_child_type, const char *part) {
    if (strcmp(STR_COMMITS, part) == 0) {
        *root_child_type = COMMIT;

        return 0;
    }

    if (strcmp(STR_BLOBS, part) == 0) {
        *root_child_type = BLOB;

        return 0;
    }

    return 1;
}

static int path_parse_commit_sub_path(struct path_spec *path_spec, int level) {
    char *ref;

    ref = path_spec->parts[level];

    if (strlen(ref) != GIT_OBJECT_ID_LEN) {
        return ENOENT;
    }

    if (++level == path_spec->parts_count) {
        path_spec->entry_type = ID;

        return 0;
    }

    char *commit_dir_item = path_spec->parts[level];

    if (strcmp(STR_TREE, commit_dir_item) == 0) {
        if (++level == path_spec->parts_count) {
            path_spec->entry_type = TREE;

            return 0;
        }

        path_spec->entry_type = PATH_IN_GIT;

        return 0;
    } else if (strcmp(STR_PARENT, commit_dir_item) == 0) {
        if (++level == path_spec->parts_count) {
            path_spec->entry_type = PARENT;

            return 0;
        }
    }

    return ENOENT;
}

static int path_parse_blob_sub_path(struct path_spec *path_spec, int level) {
    char *ref;

    ref = path_spec->parts[level];

    if (strlen(ref) != GIT_OBJECT_ID_LEN) {
        return ENOENT;
    }

    if (++level == path_spec->parts_count) {
        path_spec->entry_type = ID;

        return 0;
    }

    return ENOENT;
}

static int parse_path_init_dir_entry_type(struct path_spec *path_spec) {
    int level = 0;

    if (level == path_spec->parts_count) {
        path_spec->root_child_type = ROOT;
        path_spec->entry_type = NONE;

        return 0;
    }

    enum root_child_type root_child_type;

    if (parse_path_info_resolve_root_child(&root_child_type, path_spec->parts[level]) != 0) {
        return ENOENT;
    }

    path_spec->root_child_type = root_child_type;

    if (++level == path_spec->parts_count) {
        path_spec->entry_type = LIST;

        return 0;
    }

    switch (root_child_type) {
        case COMMIT:
            return path_parse_commit_sub_path(path_spec, level);
        case  BLOB:
            return path_parse_blob_sub_path(path_spec, level);
        default:
            HALT("Unexpected %s for path %s", root_child_type_to_str(root_child_type), path_spec_full_path(path_spec));
    }

    return ENOENT;
}

static int parse_path_as_root(struct path_spec **path_spec) {
    struct path_spec *new_path_spec = NULL;

    new_path_spec = (struct path_spec *) malloc(sizeof(struct path_spec));

    if (NULL == new_path_spec) {
        return ENOMEM;
    }

    new_path_spec->parts = NULL;
    new_path_spec->buff = NULL;
    new_path_spec->parts_count = 0;

    int path_parse_result = -parse_path_init_dir_entry_type(new_path_spec);

    if (0 != path_parse_result) {
        free(new_path_spec);

        return path_parse_result;
    }

    *path_spec = new_path_spec;

    return 0;
}

static int path_parse_as_root_child(struct path_spec *path_spec) {
    char **parts = path_spec->parts;

    char *start = path_spec->buff;
    char *current = start;
    int i = 0;

    while (*current) {
        if ('/' == *current) {
            if (current == start) {
                return ENOENT;
            }

            *current = 0;
            parts[i++] = start;

            start = current + 1;
        }

        current++;
    };

    if (current == start) {
        return ENOENT;
    }

    parts[i++] = start;

    return parse_path_init_dir_entry_type(path_spec);
}

static int parse_path(struct path_spec **path_spec, const char *path) {
    int path_len = strlen(path);

    if (0 == path_len || '/' != *path) {
        return ENOENT;
    }

    if (strcmp(path, "/") == 0) {
        return parse_path_as_root(path_spec);
    }

    struct path_spec *new_path_spec = NULL;

    int parts_count = count_char_in_string(path, '/');

    // keep it all as one chunk of memory
    void *buff = malloc(sizeof(struct path_spec) + parts_count * sizeof(char *) + strlen(path) * sizeof(char));

    if (NULL == buff) {
        return ENOMEM;
    }

    new_path_spec = (struct path_spec *) buff;
    new_path_spec->parts = (char **) (buff + sizeof(struct path_spec));
    new_path_spec->buff = (char *) (buff + sizeof(struct path_spec) + parts_count * sizeof(char *));

    memcpy(new_path_spec->buff, path + 1, path_len * sizeof(char));

    new_path_spec->parts_count = parts_count;

    int path_parse_result = path_parse_as_root_child(new_path_spec);

    if (0 != path_parse_result) {
        free(new_path_spec);

        return path_parse_result;
    }

    *path_spec = new_path_spec;

    return 0;
}

static void cleanup_on_exit_cb() {
    if (NULL != repo) {
        git_repository_free(repo);

        repo = NULL;
    }

    git_libgit2_shutdown();

    if (NULL != repo_path) {
        free(repo_path);

        repo_path = NULL;
    }

    if (args.argc > 0) {
        fuse_opt_free_args(&args);
    }
}

static int fuse_args_process_cb(void *data, const char *arg, int key, struct fuse_args *out_args) {
    (void) data;
    (void) out_args;

    if (FUSE_OPT_KEY_NONOPT == key && NULL == repo_path) {
        repo_path = strdup(arg);

        return 0;
    }

    return 1;
}

static void grofs_getattr_init_stat_as_dir(struct stat *stat, time_t started_time) {
    stat->st_atime = started_time;
    stat->st_mtime = started_time;
    stat->st_mode = S_IFDIR | 0555;
    stat->st_nlink = 2;
}

static void grofs_getattr_init_stat_as_file(struct stat *stat, time_t started_time, int size) {
    stat->st_atime = started_time;
    stat->st_mtime = started_time;
    stat->st_mode = S_IFREG | 0444;
    stat->st_nlink = 1;
    stat->st_size = size;
}

static int grofs_git_commit_parent_lookup(const git_oid *commit_oid, git_oid *parent_oid) {
    git_commit *commit;

    if (git_commit_lookup(&commit, repo, commit_oid) != 0) {
        return 1;
    }

    if (git_commit_parentcount(commit) == 0) {
        git_commit_free(commit);

        return 1;
    }

    git_oid_cpy(parent_oid, git_commit_parent_id(commit, 0));

    git_commit_free(commit);

    return 0;
}

static int grofs_git_commit_has_parent(const git_oid *commit_oid) {
    git_oid oid;

    return grofs_git_commit_parent_lookup(commit_oid, &oid) == 0;
}

static int grofs_resolve_node_for_path_spec_for_commit_children(struct grofs_node *node, const git_commit *commit, const struct path_spec *path_spec) {
    node->time = git_commit_time(commit);

    if (ID == path_spec->entry_type) {
        node->type = DIR;

        return 0;
    }

    git_tree *tree;

    if (git_commit_tree(&tree, commit) != 0) {
        return -ENOENT;
    }

    if (TREE == path_spec->entry_type) {
        node->type = DIR;

        git_oid_cpy(&node->oid, git_tree_id(tree));

        git_tree_free(tree);

        return 0;
    }

    if (PARENT == path_spec->entry_type) {
        git_oid parent_oid;
        if (grofs_git_commit_parent_lookup(git_commit_id(commit), &parent_oid) != 0) {
            return -ENOENT;
        }

        git_oid_cpy(&node->oid, &parent_oid);

        node->type = DATA;
        node->size = GIT_OID_HEXSZ;

        git_tree_free(tree);

        return 0;
    }

    git_tree_entry *tree_entry;

    char *path = path_spec_git_path(path_spec);

    if (git_tree_entry_bypath(&tree_entry, tree, path) != 0) {
        free(path);

        git_tree_free(tree);

        return -ENOENT;
    }

    free(path);

    git_oid_cpy(&node->oid, git_tree_entry_id(tree_entry));

    switch (git_tree_entry_type(tree_entry)) {
        case GIT_OBJ_TREE:
            node->type = DIR;

            break;
        case GIT_OBJ_BLOB:

            node->type = DATA;

            git_blob *blob;
            git_blob_lookup(&blob, repo, git_tree_entry_id(tree_entry));

            node->size = git_blob_rawsize(blob);

            git_blob_free(blob);
            break;
        default:
            HALT("Unexpected %s (sha1 %s)", git_object_type2string(git_tree_entry_type(tree_entry)), git_oid_tostr_s(git_tree_entry_id(tree_entry)));
    }

    git_tree_entry_free(tree_entry);

    git_tree_free(tree);

    return 0;
}

static int grofs_resolve_node_for_path_spec_for_commit_type(struct grofs_node *node, const struct path_spec *path_spec) {
    const char *id = path_spec_commit_name(path_spec);

    git_commit *commit;

    git_oid_fromstr(&node->oid, id);

    if (0 != git_commit_lookup(&commit, repo, &node->oid)) {
        return -ENOENT;
    }

    int ret = grofs_resolve_node_for_path_spec_for_commit_children(node, commit, path_spec);

    git_commit_free(commit);

    return ret;
}

static int grofs_resolve_node_for_path_spec_for_blob_type(struct grofs_node *node, const struct path_spec *path_spec) {
    if (ID != path_spec->entry_type) {
        return -ENOENT;
    }

    git_oid oid;
    git_oid_fromstr(&oid, path_spec_blob_name(path_spec));

    git_blob *blob;
    git_blob_lookup(&blob, repo, &oid);

    git_oid_cpy(&node->oid, &oid);

    node->size = git_blob_rawsize(blob);

    node->type = DATA;

    git_blob_free(blob);

    return 0;
}

static int grofs_resolve_node_for_path_spec(struct grofs_node *node, const struct path_spec *path_spec) {
    int ret = -ENOENT;

    if (ROOT == path_spec->root_child_type || LIST == path_spec->entry_type) {
        node->type = DIR;

        return 0;
    }

    switch (path_spec->root_child_type) {
        case COMMIT:
            ret = grofs_resolve_node_for_path_spec_for_commit_type(node, path_spec);

            break;
        case BLOB:
            ret = grofs_resolve_node_for_path_spec_for_blob_type(node, path_spec);

            break;
        default:
            HALT("Unexpected %s for path %s", root_child_type_to_str(path_spec->root_child_type), path_spec_full_path(path_spec));
    }

    return ret;
}

static int grofs_resolve_node_for_path(struct grofs_node **node, const char *path) {
    struct grofs_node *new_node = (struct grofs_node *) malloc(sizeof(struct grofs_node));

    if (NULL == new_node) {
        return -ENOMEM;
    }

    struct path_spec *path_spec;

    int ret = -parse_path(&path_spec, path);

    if (ret != 0) {
        return ret;
    }

    new_node->root_child_type = path_spec->root_child_type;
    new_node->entry_type = path_spec->entry_type;
    new_node->size = 0;
    new_node->time = started_time;

    ret = grofs_resolve_node_for_path_spec(new_node, path_spec);

    free_path_spec(path_spec);

    *node = new_node;

    return ret;
}

static int grofs_getattr(const char *path, struct stat *stat) {
    struct fuse_context *fuse_context = fuse_get_context();

    struct grofs_node *node;

    int ret = grofs_resolve_node_for_path(&node, path);

    if (0 != ret) {
        return ret;
    }

    // @todo: remote globals through fuse data
    stat->st_uid = fuse_context->uid;
    stat->st_gid = fuse_context->gid;

    switch (node->type) {
        case DATA:
            grofs_getattr_init_stat_as_file(stat, node->time, node->size);

            break;
        case DIR:
            grofs_getattr_init_stat_as_dir(stat, node->time);

            break;
        default:
            HALT("Unexpected %s for path %s", grofs_node_type_to_str(node->type), path);
    }

    free(node);

    return ret;
}

static int grofs_readdir_git_collect_object_cb(const git_oid *id, void *payload) {
    git_object *obj;

    struct grofs_readdir_context *context = (struct grofs_readdir_context *) payload;

    if (0 != git_object_lookup(&obj, repo, id, context->wanted_type)) {
        return 0;
    }

    char sha[GIT_OID_HEXSZ + 1];

    git_oid_tostr(sha, GIT_OID_HEXSZ + 1, id);

    context->filler(context->buffer, sha, NULL, 0);

    git_object_free(obj);

    return 0;
}

static int grofs_readdir_from_git_tree(const git_tree *tree, void *buffer, fuse_fill_dir_t filler) {
    size_t count = git_tree_entrycount(tree);

    size_t i = 0;

    for (i = 0; i < count; i++) {
        const git_tree_entry *entry = git_tree_entry_byindex(tree, i);

        if (NULL == entry) {
            return 1;
        }

        git_otype entry_type = git_tree_entry_type(entry);

        if (GIT_OBJ_BLOB != entry_type && GIT_OBJ_TREE != entry_type) {
            continue ;
        }

        filler(buffer, git_tree_entry_name(entry), NULL, 0);
    }

    return 0;
}

static int grofs_readdir_from_git_tree_oid(const git_oid *oid, void *buffer, fuse_fill_dir_t filler) {
    git_tree *tree;
    if (git_tree_lookup(&tree, repo, oid) != 0) {
        return -ENOENT;
    }

    int ret = grofs_readdir_from_git_tree(tree, buffer, filler);

    git_tree_free(tree);

    return ret;
}

static int grofs_readdir_for_commit_list(void *buffer, fuse_fill_dir_t filler) {
    struct grofs_readdir_context context;
    context.filler = filler;
    context.buffer = buffer;
    context.wanted_type = GIT_OBJ_COMMIT;

    git_odb *odb;
    git_repository_odb(&odb, repo);

    int ret = git_odb_foreach(odb, grofs_readdir_git_collect_object_cb, &context);

    git_odb_free(odb);

    return ret;
}

static int grofs_readdir_for_blob_list(void *buffer, fuse_fill_dir_t filler) {
    struct grofs_readdir_context context;
    context.filler = filler;
    context.buffer = buffer;
    context.wanted_type = GIT_OBJ_BLOB;

    git_odb *odb;
    git_repository_odb(&odb, repo);

    int ret = git_odb_foreach(odb, grofs_readdir_git_collect_object_cb, &context);

    git_odb_free(odb);

    return ret;
}

static int grofs_readdir_for_node(const struct grofs_node *node, void *buffer, fuse_fill_dir_t filler) {
    if (ROOT == node->root_child_type) {
        filler(buffer, STR_COMMITS, NULL, 0);
        filler(buffer, STR_BLOBS, NULL, 0);

        return 0;
    } else if (COMMIT == node->root_child_type && LIST == node->entry_type) {
        return grofs_readdir_for_commit_list(buffer, filler);
    } else if (BLOB == node->root_child_type && LIST == node->entry_type) {
        return grofs_readdir_for_blob_list(buffer, filler);
    } else if (COMMIT == node->root_child_type && TREE == node->entry_type) {
        return grofs_readdir_from_git_tree_oid(&node->oid, buffer, filler);
    } else if (COMMIT == node->root_child_type && ID == node->entry_type) {
        filler(buffer, STR_TREE, NULL, 0);

        if (grofs_git_commit_has_parent(&node->oid)) {
            filler(buffer, STR_PARENT, NULL, 0);
        }

        return 0;
    } else if (PATH_IN_GIT == node->entry_type) {
        return grofs_readdir_from_git_tree_oid(&node->oid, buffer, filler);
    }

    return -ENOENT;
}

static int grofs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *file_info) {
    (void) offset;
    (void) file_info;

    struct grofs_node *node;

    int ret = grofs_resolve_node_for_path(&node, path);

    if (0 != ret) {
        return ret;
    }

    filler(buffer, ".", NULL, 0);
    filler(buffer, "..", NULL, 0);

    ret = -ENOENT;

    if (DATA == node->type) {
        ret = -ENOTDIR;
    } else if (DIR == node->type) {
        ret = grofs_readdir_for_node(node, buffer, filler);
    }

    free(node);

    return ret;
}

static struct grofs_file_handle *grofs_file_nandle_new(int buff_len) {
    void *buff = malloc(sizeof(struct grofs_file_handle) + sizeof(char) * buff_len);

    struct grofs_file_handle *file_handle = (struct grofs_file_handle *) buff;

    file_handle->buff = buff + sizeof(struct grofs_file_handle);
    file_handle->len = buff_len;

    return file_handle;
}

static void grofs_file_handle_free(struct grofs_file_handle *file_handle) {
    free(file_handle);
}

static int grofs_open_node_commit_parent(const git_oid *oid, struct fuse_file_info * file_info) {
    struct grofs_file_handle *file_handle = grofs_file_nandle_new(GIT_OID_HEXSZ);

    git_oid_nfmt(file_handle->buff, GIT_OID_HEXSZ, oid);

    file_info->fh = (uint64_t) file_handle;

    return 0;
}

static int grofs_open_node_blob(const git_oid *oid, struct fuse_file_info *file_info) {
    git_blob *blob;

    if (git_blob_lookup(&blob, repo, oid) != 0) {
        return -ENOENT;
    }

    int size = git_blob_rawsize(blob);

    struct grofs_file_handle *file_handle = grofs_file_nandle_new(size);

    memcpy(file_handle->buff, git_blob_rawcontent(blob), size);

    git_blob_free(blob);

    file_info->fh = (uint64_t) file_handle;

    return 0;
}

static int grofs_open_node(const struct grofs_node *node, struct fuse_file_info *file_info) {
    if (COMMIT == node->root_child_type && PARENT == node->entry_type) {
        return grofs_open_node_commit_parent(&node->oid, file_info);
    } else if (PATH_IN_GIT == node->entry_type) {
        return grofs_open_node_blob(&node->oid, file_info);
    } else if (BLOB == node->root_child_type && ID == node->entry_type) {
        return grofs_open_node_blob(&node->oid, file_info);
    }

    return -ENOENT;
}

static int grofs_open(const char *path, struct fuse_file_info *file_info) {
    struct grofs_node *node;

    if (grofs_resolve_node_for_path(&node, path) != 0) {
        return -ENOENT;
    }

    int ret = -ENOENT;

    if (DIR == node->type) {
        ret = -EISDIR;
    } else if (DATA == node->type) {
        ret = grofs_open_node(node, file_info);
    }

    free(node);

    return ret;
}

static int grofs_read(const char *path, char *buff, size_t size, off_t offset, struct fuse_file_info *file_info) {
    (void) path;

    struct grofs_file_handle *file_handle = (struct grofs_file_handle *) file_info->fh;

    if (offset >= file_handle->len) {
        return 0;
    }

    int to_read = min(size, file_handle->len - offset);

    memcpy(buff, file_handle->buff + offset, sizeof(char) * to_read);

    return to_read;
}

static int grofs_release(const char* path, struct fuse_file_info *file_info) {
    (void) path;

    grofs_file_handle_free((struct grofs_file_handle *) file_info->fh);

    return 0;
}

static void grofs_print_help(const char *bin_path) {
    const char *help_format =
        "usage: %s git-repo-path mount-point [options]\n"
        "\n"
        "Mounts local Git repository and exposes commits/blobs as folders/files.\n"
        "\n"
        "grofs options:\n"
        "    -h   --help            print help\n"
        "    -V   --version         print version\n"
        "\n";

    fprintf(stderr, help_format, bin_path);
}

int main(int argc, char **argv) {
    started_time = time(NULL);

    git_libgit2_init();

    atexit(cleanup_on_exit_cb);

    args.argc = argc;
    args.argv = argv;
    args.allocated = 0;

    struct grofs_cli_opts cli_opts = {0, 0};

    if(fuse_opt_parse(&args, &cli_opts, grofs_fuse_opts, fuse_args_process_cb) == -1) {
        fprintf(stderr, "Failed to parse options");

        return 1;
    }

    if (cli_opts.show_help) {
        grofs_print_help(args.argv[0]);

        fuse_opt_add_arg(&args, "-ho");

        return fuse_main(args.argc, args.argv, &fuse_operations, NULL);
    }

    if (cli_opts.show_version) {
        fprintf(stderr, "grofs version: %s\n", VERSION);

        fuse_opt_add_arg(&args, "--version");

        return fuse_main(args.argc, args.argv, &fuse_operations, NULL);
    }

    if (NULL == repo_path) {
        fprintf(stderr, "Git repository path not provided\n\n");

        grofs_print_help(args.argv[0]);

        return 1;
    }

    int error = git_repository_open(&repo, repo_path);

    if (0 != error) {
        fprintf(stderr, "Failed to find Git repository at path: %s\n", repo_path);

        return 1;
    }

    return fuse_main(args.argc, args.argv, &fuse_operations, NULL);
}
