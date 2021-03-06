#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <git2.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <stddef.h>
#include <unistd.h>
#include <pthread.h>
#include <threads.h>
#include <fcntl.h>
#include <errno.h>

#define FUSE_USE_VERSION 30

#include <fuse.h>

#define GROFS_STR_COMMITS "commits"
#define GROFS_STR_BLOBS "blobs"
#define GROFS_STR_TREE "tree"
#define GROFS_STR_PARENT "parent"

#define GROFS_VERSION "0.1.0-alpha"

#define GROFS_GIT_OBJECT_ID_LEN GIT_OID_HEXSZ

#define GROFS_LOGIC_ERROR 64

// Halts in case of a logic error because it's better to exit since we don't know what else could be wrong
#define GROFS_HALT_FMT(fmt, ...) fprintf(stderr, "ERROR [file: %s, line: %d]: " fmt, __FILE__, __LINE__, __VA_ARGS__); exit(GROFS_LOGIC_ERROR)
#define GROFS_HALT(fmt) GROFS_HALT_FMT(fmt "%s", "")

#define FUSE_ERR(r) -(r)

enum grofs_dir_entry_type {
    NONE, LIST, ID, PATH_IN_GIT, TREE, PARENT
};

enum grofs_root_child_type {
    ROOT, COMMIT, BLOB
};

struct grofs_path_spec {
    char *buff;
    char **parts;
    int parts_count;
    enum grofs_dir_entry_type entry_type;
    enum grofs_root_child_type root_child_type;
};

enum grofs_node_type {
    DATA /* since I can't have FILE */, DIR
};

struct grofs_node {
    enum grofs_node_type type;
    enum grofs_dir_entry_type entry_type;
    enum grofs_root_child_type root_child_type;
    git_oid oid;
    time_t time;
    size_t size;
};

struct grofs_readdir_context {
    int fd;
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

struct grofs_buff {
    char *data;
    size_t pos;
    size_t len;
    size_t size;
};

struct grofs_readdir_thread_data {
    int fd;
    void (*iter) (int fd, void *);
    void *iter_payload; // must be a valid pointer to be freed later or NULL
    int should_stop;
};

struct grofs_dir_handle {
    pthread_t read_thr;
    int fd;
    struct grofs_readdir_thread_data thread_data;
    off_t last_offset;
    struct grofs_buff buff;
};


#define GROFS_READDIR_BUFF_LEN 64

static const char *grofs_root_child_type_to_str(enum grofs_root_child_type type);
static char *grofs_path_spec_full_path(const struct grofs_path_spec *path_spec);
static char *grofs_path_spec_sub_path(const struct grofs_path_spec *path_spec, int start_part);
static const char *grofs_path_spec_blob_name(const struct grofs_path_spec *path_spec);
static char *grofs_path_spec_git_path(const struct grofs_path_spec *path_spec);
static inline int grofs_min(int a, int b);
static void grofs_free_path_spec(struct grofs_path_spec *path_spec);
static int grofs_count_char_in_string(const char *str, char needle);
static int grofs_parse_path_info_resolve_root_child(enum grofs_root_child_type *root_child_type, const char *part);
static int grofs_path_parse_commit_sub_path(struct grofs_path_spec *path_spec, int level);
static int grofs_path_parse_blob_sub_path(struct grofs_path_spec *path_spec, int level);
static int grofs_parse_path_init_dir_entry_type(struct grofs_path_spec *path_spec);
static int grofs_parse_path_as_root(struct grofs_path_spec **path_spec);
static int grofs_path_parse_as_root_child(struct grofs_path_spec *path_spec);
static int grofs_parse_path(struct grofs_path_spec **path_spec, const char *path);
static void grofs_cleanup_on_exit_cb();
static int grofs_fuse_args_process_cb(void *data, const char *arg, int key, struct fuse_args *out_args);
static int grofs_git_commit_parent_lookup(const git_oid *commit_oid, git_oid *parent_oid);
static int grofs_git_commit_has_parent(const git_oid *commit_oid);
static void grofs_getattr_init_stat_as_dir(struct stat *stat, time_t started_time);
static void grofs_getattr_init_stat_as_file(struct stat *stat, time_t started_time, int size);
static int grofs_resolve_node_for_path_spec_for_commit_children(struct grofs_node *node, const git_commit *commit, const struct grofs_path_spec *path_spec);
static int grofs_resolve_node_for_path_spec_for_commit_type(struct grofs_node *node, const struct grofs_path_spec *path_spec);
static int grofs_resolve_node_for_path_spec_for_blob_type(struct grofs_node *node, const struct grofs_path_spec *path_spec);
static int grofs_node_init_from_path(struct grofs_node *node, const char *path);
static int grofs_write_bin_with_local_cancel(int fd, const char *data);
static void grofs_dir_iter_root(int fd, void *iter_payload);
static void grofs_dir_iter_commit_id(int fd, void *iter_payload);
static void grofs_dir_iter_commit_list(int fd, void *iter_payload);
static void grofs_dir_iter_for_blob_list(int fd, void *iter_payload);
static void grofs_dir_iter_for_blob_list_tree(int fd, void *iter_payload);
static void *grofs_readdir_thread(void *data);
static int grofs_open_dir_create_thread_data(struct grofs_readdir_thread_data *thread_data, const struct grofs_node *node);
static int grofs_spawn_read_thread(struct grofs_dir_handle *dir_handle);
static int grofs_opendir_create_dir_handle_from_node(struct grofs_dir_handle **dir_handle, struct grofs_node *node);
static int grofs_fill_from_dir_handle(struct grofs_dir_handle *dir_handle, void *buffer, fuse_fill_dir_t filler);
static void grofs_buff_align_to_start(struct grofs_buff *buff);
static int grofs_buff_realloc(struct grofs_buff *buff);
static int grofs_resolve_node_for_path_spec(struct grofs_node *node, const struct grofs_path_spec *path_spec);
static int grofs_resolve_node_for_path(struct grofs_node **node, const char *path);
static int grofs_readdir_git_collect_object_cb(const git_oid *id, void *payload);
static struct grofs_file_handle *grofs_file_nandle_new(int buff_len);
static int grofs_open_node_commit_parent(const git_oid *oid, struct fuse_file_info * file_info);
static int grofs_open_node_blob(const git_oid *oid, struct fuse_file_info *file_info);
static int grofs_open_node(const struct grofs_node *node, struct fuse_file_info *file_info);
static void grofs_releasedir_close_thread(int read_fd, pthread_t read_thr, struct grofs_readdir_thread_data *thread_data);
static void grofs_print_help(const char *bin_path);

static int grofs_getattr(const char *path, struct stat *stat);
static int grofs_opendir(const char *path, struct fuse_file_info *file_info);
static int grofs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *file_info);
static int grofs_releasedir(const char *path, struct fuse_file_info *file_info);
static int grofs_open(const char *path, struct fuse_file_info *file_info);
static int grofs_read(const char *path, char *buff, size_t size, off_t offset, struct fuse_file_info *file_info);
static int grofs_release(const char* path, struct fuse_file_info *file_info);

static char *grofs_repo_path = NULL;
static git_repository *grofs_repo = NULL;
static time_t grofs_started_time;
struct fuse_args grofs_args = FUSE_ARGS_INIT(0, NULL);

thread_local int *grofs_should_stop_local;

struct fuse_operations grofs_fuse_operations = {
    .getattr	= grofs_getattr,
    .opendir	= grofs_opendir,
    .readdir	= grofs_readdir,
    .releasedir	= grofs_releasedir,
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

static const char *grofs_root_child_type_to_str(enum grofs_root_child_type type) {
    switch (type) {
        case ROOT:
            return "ROOT";
        case COMMIT:
            return "COMMIT";
        case BLOB:
            return "BLOB";
        default:
            GROFS_HALT_FMT("Unknown %d", type);
    }
}

static const char *grofs_node_type_to_str(enum grofs_node_type type) {
    switch (type) {
        case DIR:
            return "DIR";
        case DATA:
            return "DATA";
        default:
            GROFS_HALT_FMT("Unknown %d", type);
    }
}

static const char *path_spec_commit_name(const struct grofs_path_spec *path_spec) {
    if (path_spec->parts_count < 2 || COMMIT != path_spec->root_child_type) {
        return NULL;
    }

    return path_spec->parts[1];
}

static const char *grofs_path_spec_blob_name(const struct grofs_path_spec *path_spec) {
    if (path_spec->parts_count < 2 || BLOB != path_spec->root_child_type) {
        return NULL;
    }

    return path_spec->parts[1];
}

static char *grofs_path_spec_sub_path(const struct grofs_path_spec *path_spec, int start_part) {
    if (start_part < 0 || start_part >= path_spec->parts_count) {
        return NULL;
    }

    size_t path_len = 1;

    char *last_part = path_spec->parts[path_spec->parts_count - 1];

    void *parts_buff_end = last_part + strlen(last_part);

    path_len += (parts_buff_end - (void *) *(path_spec->parts + start_part));

    char *buff = (char *) malloc(sizeof(char) * (path_len + 1));

    if (NULL == buff) {
        return NULL;
    }

    *buff = '/';
    *(buff + 1) = '\0';

    memcpy(buff + 1, *(path_spec->parts + start_part), sizeof(char) * (path_len - 1 + 1));

    size_t i;

    for (i = 1; i < path_len; i++) {
        if ('\0' == *(buff + i)) {
            *(buff + i) = '/';
        }
    }

    return buff;
}

static char *grofs_path_spec_git_path(const struct grofs_path_spec *path_spec) {
    return grofs_path_spec_sub_path(path_spec, 3);
}

static char *grofs_path_spec_full_path(const struct grofs_path_spec *path_spec) {
    if (path_spec->parts_count > 0) {
        return grofs_path_spec_sub_path(path_spec, 0);
    }

    char *buff = (char *) malloc(sizeof(char) * (1 + 1));

    if (NULL == buff) {
        return NULL;
    }

    *buff = '/';
    *(buff + 1) = '\0';

    return buff;
}

static inline int grofs_min(int a, int b) {
    return a < b ? a : b;
}

static void grofs_free_path_spec(struct grofs_path_spec *path_spec) {
    free(path_spec);
}

static int grofs_count_char_in_string(const char *str, char needle) {
    int count = 0;

    for (; *str; count += needle == *str, str++);

    return count;
}

static int grofs_parse_path_info_resolve_root_child(enum grofs_root_child_type *root_child_type, const char *part) {
    if (strcmp(GROFS_STR_COMMITS, part) == 0) {
        *root_child_type = COMMIT;

        return 0;
    }

    if (strcmp(GROFS_STR_BLOBS, part) == 0) {
        *root_child_type = BLOB;

        return 0;
    }

    return 1;
}

static int grofs_path_parse_commit_sub_path(struct grofs_path_spec *path_spec, int level) {
    char *ref;

    ref = path_spec->parts[level];

    if (strlen(ref) != GROFS_GIT_OBJECT_ID_LEN) {
        return ENOENT;
    }

    if (++level == path_spec->parts_count) {
        path_spec->entry_type = ID;

        return 0;
    }

    char *commit_dir_item = path_spec->parts[level];

    if (strcmp(GROFS_STR_TREE, commit_dir_item) == 0) {
        if (++level == path_spec->parts_count) {
            path_spec->entry_type = TREE;

            return 0;
        }

        path_spec->entry_type = PATH_IN_GIT;

        return 0;
    } else if (strcmp(GROFS_STR_PARENT, commit_dir_item) == 0) {
        if (++level == path_spec->parts_count) {
            path_spec->entry_type = PARENT;

            return 0;
        }
    }

    return ENOENT;
}

static int grofs_path_parse_blob_sub_path(struct grofs_path_spec *path_spec, int level) {
    char *ref;

    ref = path_spec->parts[level];

    if (strlen(ref) != GROFS_GIT_OBJECT_ID_LEN) {
        return ENOENT;
    }

    if (++level == path_spec->parts_count) {
        path_spec->entry_type = ID;

        return 0;
    }

    return ENOENT;
}

static int grofs_parse_path_init_dir_entry_type(struct grofs_path_spec *path_spec) {
    int level = 0;

    if (level == path_spec->parts_count) {
        path_spec->root_child_type = ROOT;
        path_spec->entry_type = NONE;

        return 0;
    }

    enum grofs_root_child_type root_child_type;

    if (grofs_parse_path_info_resolve_root_child(&root_child_type, path_spec->parts[level]) != 0) {
        return ENOENT;
    }

    path_spec->root_child_type = root_child_type;

    if (++level == path_spec->parts_count) {
        path_spec->entry_type = LIST;

        return 0;
    }

    switch (root_child_type) {
        case COMMIT:
            return grofs_path_parse_commit_sub_path(path_spec, level);
        case  BLOB:
            return grofs_path_parse_blob_sub_path(path_spec, level);
        default:
            GROFS_HALT_FMT("Unexpected %s for path %s", grofs_root_child_type_to_str(root_child_type), grofs_path_spec_full_path(path_spec));
    }

    return ENOENT;
}

static int grofs_parse_path_as_root(struct grofs_path_spec **path_spec) {
    struct grofs_path_spec *new_path_spec = NULL;

    new_path_spec = (struct grofs_path_spec *) malloc(sizeof(struct grofs_path_spec));

    if (NULL == new_path_spec) {
        return ENOMEM;
    }

    new_path_spec->parts = NULL;
    new_path_spec->buff = NULL;
    new_path_spec->parts_count = 0;

    int grofs_path_parse_result = -grofs_parse_path_init_dir_entry_type(new_path_spec);

    if (0 != grofs_path_parse_result) {
        free(new_path_spec);

        return grofs_path_parse_result;
    }

    *path_spec = new_path_spec;

    return 0;
}

static int grofs_path_parse_as_root_child(struct grofs_path_spec *path_spec) {
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

    return grofs_parse_path_init_dir_entry_type(path_spec);
}

static int grofs_parse_path(struct grofs_path_spec **path_spec, const char *path) {
    int path_len = strlen(path);

    if (0 == path_len || '/' != *path) {
        return ENOENT;
    }

    if (strcmp(path, "/") == 0) {
        return grofs_parse_path_as_root(path_spec);
    }

    struct grofs_path_spec *new_path_spec = NULL;

    int parts_count = grofs_count_char_in_string(path, '/');

    // keep it all as one chunk of memory
    void *buff = malloc(sizeof(struct grofs_path_spec) + parts_count * sizeof(char *) + strlen(path) * sizeof(char));

    if (NULL == buff) {
        return ENOMEM;
    }

    new_path_spec = (struct grofs_path_spec *) buff;
    new_path_spec->parts = (char **) (buff + sizeof(struct grofs_path_spec));
    new_path_spec->buff = (char *) (buff + sizeof(struct grofs_path_spec) + parts_count * sizeof(char *));

    memcpy(new_path_spec->buff, path + 1, path_len * sizeof(char));

    new_path_spec->parts_count = parts_count;

    int grofs_path_parse_result = grofs_path_parse_as_root_child(new_path_spec);

    if (0 != grofs_path_parse_result) {
        free(new_path_spec);

        return grofs_path_parse_result;
    }

    *path_spec = new_path_spec;

    return 0;
}

static void grofs_cleanup_on_exit_cb() {
    if (NULL != grofs_repo) {
        git_repository_free(grofs_repo);

        grofs_repo = NULL;
    }

    git_libgit2_shutdown();

    if (NULL != grofs_repo_path) {
        free(grofs_repo_path);

        grofs_repo_path = NULL;
    }

    if (grofs_args.argc > 0) {
        fuse_opt_free_args(&grofs_args);
    }
}

static int grofs_fuse_args_process_cb(void *data, const char *arg, int key, struct fuse_args *out_args) {
    (void) data;
    (void) out_args;

    if (FUSE_OPT_KEY_NONOPT == key && NULL == grofs_repo_path) {
        grofs_repo_path = strdup(arg);

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

    if (git_commit_lookup(&commit, grofs_repo, commit_oid) != 0) {
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

static int grofs_resolve_node_for_path_spec_for_commit_children(struct grofs_node *node, const git_commit *commit, const struct grofs_path_spec *path_spec) {
    node->time = git_commit_time(commit);

    if (ID == path_spec->entry_type) {
        node->type = DIR;

        return 0;
    }

    git_tree *tree;

    if (git_commit_tree(&tree, commit) != 0) {
        return ENOENT;
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
            return ENOENT;
        }

        git_oid_cpy(&node->oid, &parent_oid);

        node->type = DATA;
        node->size = GIT_OID_HEXSZ;

        git_tree_free(tree);

        return 0;
    }

    git_tree_entry *tree_entry;

    char *path = grofs_path_spec_git_path(path_spec);

    if (NULL == path) {
        git_tree_free(tree);

        return ENOENT;
    }

    if (git_tree_entry_bypath(&tree_entry, tree, path + 1) != 0) {
        free(path);

        git_tree_free(tree);

        return ENOENT;
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
            git_blob_lookup(&blob, grofs_repo, git_tree_entry_id(tree_entry));

            node->size = git_blob_rawsize(blob);

            git_blob_free(blob);
            break;
        default:
            GROFS_HALT_FMT("Unexpected %s (sha1 %s)", git_object_type2string(git_tree_entry_type(tree_entry)), git_oid_tostr_s(git_tree_entry_id(tree_entry)));
    }

    git_tree_entry_free(tree_entry);

    git_tree_free(tree);

    return 0;
}

static int grofs_resolve_node_for_path_spec_for_commit_type(struct grofs_node *node, const struct grofs_path_spec *path_spec) {
    const char *id = path_spec_commit_name(path_spec);

    git_commit *commit;

    git_oid_fromstr(&node->oid, id);

    if (0 != git_commit_lookup(&commit, grofs_repo, &node->oid)) {
        return ENOENT;
    }

    int ret = grofs_resolve_node_for_path_spec_for_commit_children(node, commit, path_spec);

    git_commit_free(commit);

    return ret;
}

static int grofs_resolve_node_for_path_spec_for_blob_type(struct grofs_node *node, const struct grofs_path_spec *path_spec) {
    if (ID != path_spec->entry_type) {
        return ENOENT;
    }

    git_oid oid;
    git_oid_fromstr(&oid, grofs_path_spec_blob_name(path_spec));

    git_blob *blob;
    git_blob_lookup(&blob, grofs_repo, &oid);

    git_oid_cpy(&node->oid, &oid);

    node->size = git_blob_rawsize(blob);

    node->type = DATA;

    git_blob_free(blob);

    return 0;
}

static int grofs_resolve_node_for_path_spec(struct grofs_node *node, const struct grofs_path_spec *path_spec) {
    int ret = ENOENT;

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
            GROFS_HALT_FMT("Unexpected %s for path %s", grofs_root_child_type_to_str(path_spec->root_child_type), grofs_path_spec_full_path(path_spec));
    }

    return ret;
}

static int grofs_node_init_from_path(struct grofs_node *node, const char *path) {
    struct grofs_path_spec *path_spec;

    int ret = grofs_parse_path(&path_spec, path);

    if (ret != 0) {
        return ret;
    }

    node->root_child_type = path_spec->root_child_type;
    node->entry_type = path_spec->entry_type;
    node->size = 0;
    node->time = grofs_started_time;

    ret = grofs_resolve_node_for_path_spec(node, path_spec);

    grofs_free_path_spec(path_spec);

    return ret;
}

static int grofs_resolve_node_for_path(struct grofs_node **node, const char *path) {
    struct grofs_node *new_node = (struct grofs_node *) malloc(sizeof(struct grofs_node));

    if (NULL == new_node) {
        return ENOMEM;
    }

    int ret = grofs_node_init_from_path(new_node, path);

    if (0 == ret) {
        *node = new_node;

        return 0;
    }

    free(new_node);

    return ret;
}

static int grofs_write_bin_with_local_cancel(int fd, const char *data) {
    if (*grofs_should_stop_local) {
        return ECANCELED;
    }

    int ret = write(fd, data, strlen(data) + 1);

    if (ret < 0) {
        return errno;
    }

    return 0;
}

// Returns negative because of https://github.com/libgit2/libgit2/issues/4946
static int grofs_readdir_git_collect_object_cb(const git_oid *id, void *payload) {
    if (*grofs_should_stop_local) {
        return -ECANCELED;
    }

    git_object *obj;

    struct grofs_readdir_context *context = (struct grofs_readdir_context *) payload;

    if (0 != git_object_lookup(&obj, grofs_repo, id, context->wanted_type)) {
        return 0;
    }

    char sha[GIT_OID_HEXSZ + 1];

    git_oid_tostr(sha, GIT_OID_HEXSZ + 1, id);

    int ret = -grofs_write_bin_with_local_cancel(context->fd, sha);

    git_object_free(obj);

    return ret;
}

static void grofs_dir_iter_root(int fd, void *iter_payload) {
    (void) iter_payload;

    if (grofs_write_bin_with_local_cancel(fd, GROFS_STR_COMMITS)) {
        return ;
    }
    if (grofs_write_bin_with_local_cancel(fd, GROFS_STR_BLOBS)) {
        return ;
    }
}

static void grofs_dir_iter_commit_id(int fd, void *iter_payload) {
    grofs_write_bin_with_local_cancel(fd, GROFS_STR_TREE);

    if (grofs_git_commit_has_parent((git_oid *) iter_payload)) {
        grofs_write_bin_with_local_cancel(fd, GROFS_STR_PARENT);
    }
}

static void grofs_dir_iter_commit_list(int fd, void *iter_payload) {
    (void) iter_payload;

    struct grofs_readdir_context context = {
        .fd = fd,
        .wanted_type = GIT_OBJ_COMMIT
    };

    git_odb *odb;
    git_repository_odb(&odb, grofs_repo);

    git_odb_foreach(odb, grofs_readdir_git_collect_object_cb, &context);

    git_odb_free(odb);
}

static void grofs_dir_iter_for_blob_list(int fd, void *iter_payload) {
    (void) iter_payload;

    struct grofs_readdir_context context = {
        .fd = fd,
        .wanted_type = GIT_OBJ_BLOB
    };

    git_odb *odb;
    git_repository_odb(&odb, grofs_repo);

    git_odb_foreach(odb, grofs_readdir_git_collect_object_cb, &context);

    git_odb_free(odb);
}

static void grofs_dir_iter_for_blob_list_tree(int fd, void *iter_payload) {
    git_tree *tree = (git_tree *) iter_payload;

    size_t count = git_tree_entrycount(tree);

    size_t i = 0;

    for (i = 0; i < count; i++) {
        const git_tree_entry *entry = git_tree_entry_byindex(tree, i);

        if (NULL == entry) {
            return;
        }

        git_otype entry_type = git_tree_entry_type(entry);

        if (GIT_OBJ_BLOB != entry_type && GIT_OBJ_TREE != entry_type) {
            continue ;
        }

        const char *entry_name = git_tree_entry_name(entry);

        if (grofs_write_bin_with_local_cancel(fd, entry_name)) {
            return ;
        }
    }
}

static void grofs_dir_iter_for_blob_list_tree_oid(int fd, void *iter_payload) {
    git_tree *tree;
    if (git_tree_lookup(&tree, grofs_repo, (git_oid *) iter_payload) != 0) {
        return ;
    }

    grofs_dir_iter_for_blob_list_tree(fd, (void *) tree);

    git_tree_free(tree);
}

static void *grofs_readdir_thread(void *data) {
    struct grofs_readdir_thread_data *thread_data = (struct grofs_readdir_thread_data *) data;

    grofs_should_stop_local = &thread_data->should_stop;

    if (grofs_write_bin_with_local_cancel(thread_data->fd, ".")) {
        close(thread_data->fd);

        return NULL;
    }

    if (grofs_write_bin_with_local_cancel(thread_data->fd, "..")) {
        close(thread_data->fd);

        return NULL;
    }

    thread_data->iter(thread_data->fd, thread_data->iter_payload);

    close(thread_data->fd);

    return NULL;
}

static int grofs_open_dir_create_thread_data(struct grofs_readdir_thread_data *thread_data, const struct grofs_node *node) {
    thread_data->should_stop = 0;

    if (ROOT == node->root_child_type) {
        thread_data->iter = grofs_dir_iter_root;

        return 0;
    } else if (COMMIT == node->root_child_type && LIST == node->entry_type) {
        thread_data->iter = grofs_dir_iter_commit_list;

        return 0;
    } else if (BLOB == node->root_child_type && LIST == node->entry_type) {
        thread_data->iter = grofs_dir_iter_for_blob_list;

        return 0;
    } else if (COMMIT == node->root_child_type && ID == node->entry_type) {
        git_oid *oid = (git_oid *) malloc(sizeof(git_oid));

        if (NULL == oid) {
            return ENOMEM;
        }

        git_oid_cpy(oid, &node->oid);

        thread_data->iter = grofs_dir_iter_commit_id;
        thread_data->iter_payload = oid;

        return 0;
    } else if (
        PATH_IN_GIT == node->entry_type
        ||
        (COMMIT == node->root_child_type && TREE == node->entry_type)
    ) {
        git_oid *oid = (git_oid *) malloc(sizeof(git_oid));

        if (NULL == oid) {
            return ENOMEM;
        }

        git_oid_cpy(oid, &node->oid);

        thread_data->iter = grofs_dir_iter_for_blob_list_tree_oid;
        thread_data->iter_payload = oid;

        return 0;
    }

    return ENOENT;
}

static int grofs_spawn_read_thread(struct grofs_dir_handle *dir_handle) {
    int fds[2];

    int ret = pipe(fds);

    if (0 != ret) {
        return ret;
    }

    dir_handle->thread_data.fd = fds[1];
    dir_handle->fd = fds[0];
    dir_handle->last_offset = 0;

    ret = pthread_create(&dir_handle->read_thr, NULL, grofs_readdir_thread, &dir_handle->thread_data);

    if (0 != ret) {
        close(fds[0]);
        close(fds[1]);
    }

    return ret;
}

static int grofs_opendir_create_dir_handle_from_node(struct grofs_dir_handle **dir_handle, struct grofs_node *node) {
    struct grofs_dir_handle *new_dir_handle = (struct grofs_dir_handle *) malloc(sizeof(struct grofs_dir_handle));

    if (NULL == new_dir_handle) {
        return ENOMEM;
    }

    new_dir_handle->buff.data = (char *) malloc(sizeof(char) * GROFS_READDIR_BUFF_LEN);

    if (NULL == new_dir_handle->buff.data) {
        free(new_dir_handle);

        return ENOMEM;
    }

    new_dir_handle->buff.len = 0;
    new_dir_handle->buff.pos = 0;
    new_dir_handle->buff.size = GROFS_READDIR_BUFF_LEN;

    new_dir_handle->thread_data.iter_payload = NULL;

    int ret = grofs_open_dir_create_thread_data(&(new_dir_handle->thread_data), node);

    if (0 == ret) {
        ret = grofs_spawn_read_thread(new_dir_handle);
    }

    if (0 == ret) {
        *dir_handle = new_dir_handle;

        return 0;
    }

    free(new_dir_handle);

    return ret;
}

static int grofs_fill_from_dir_handle(struct grofs_dir_handle *dir_handle, void *buffer, fuse_fill_dir_t filler) {
    struct grofs_buff *buff = &dir_handle->buff;

    size_t start = buff->pos;

    while (start < buff->len) {
        if (*(buff->data + start) != '\0') {
            start++;

            continue;
        }

        if (start == buff->pos) {
            GROFS_HALT("Unexpected empty line");
        }

        size_t new_offset = dir_handle->last_offset + start - buff->pos;

        if (filler(buffer, buff->data + buff->pos, NULL, new_offset) == 1) {
            return 1;
        }

        buff->pos = ++start;
        dir_handle->last_offset = new_offset;
    }

    return 0;
}

static void grofs_buff_align_to_start(struct grofs_buff *buff) {
    if (buff->pos == buff->len) {
        buff->pos = 0;
        buff->len = 0;
    }

    if (buff->pos > 0) {
        memmove(buff->data, buff->data + buff->pos, buff->len - buff->pos);
        buff->len -= buff->pos;
        buff->pos = 0;
    }
}

static int grofs_buff_realloc(struct grofs_buff *buff) {
    size_t new_size = (buff->len + (GROFS_READDIR_BUFF_LEN << 1) - 1) / GROFS_READDIR_BUFF_LEN * GROFS_READDIR_BUFF_LEN;

    if (new_size == buff->size) {
        return 0;
    }

    char *new_buff = (char *) realloc(buff->data, new_size);

    if (NULL == new_buff) {
        return ENOMEM;
    }

    buff->data = new_buff;
    buff->size = new_size;

    return 0;
}

static void grofs_releasedir_close_thread(int read_fd, pthread_t read_thr, struct grofs_readdir_thread_data *thread_data) {
    thread_data->should_stop = 1;

    fcntl(read_fd, F_SETFL, fcntl(read_fd, F_GETFL) | O_NONBLOCK);

    char buff[4096];

    // @todo: would semaphores work better?
    while (read(read_fd, buff, 4096) > 0);

    pthread_join(read_thr, NULL);
}

static struct grofs_file_handle *grofs_file_nandle_new(int buff_len) {
    void *buff = malloc(sizeof(struct grofs_file_handle) + sizeof(char) * buff_len);

    if (NULL == buff) {
        return NULL;
    }

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

    if (NULL == file_handle) {
        return ENOMEM;
    }

    git_oid_nfmt(file_handle->buff, GIT_OID_HEXSZ, oid);

    file_info->fh = (uint64_t) file_handle;

    return 0;
}

static int grofs_open_node_blob(const git_oid *oid, struct fuse_file_info *file_info) {
    git_blob *blob;

    if (git_blob_lookup(&blob, grofs_repo, oid) != 0) {
        return ENOENT;
    }

    int size = git_blob_rawsize(blob);

    struct grofs_file_handle *file_handle = grofs_file_nandle_new(size);

    if (NULL == file_handle) {
        return ENOMEM;
    }

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

    return ENOENT;
}

static int grofs_getattr(const char *path, struct stat *stat) {
    struct fuse_context *fuse_context = fuse_get_context();

    struct grofs_node *node;

    int ret = grofs_resolve_node_for_path(&node, path);

    if (0 != ret) {
        return FUSE_ERR(ret);
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
        GROFS_HALT_FMT("Unexpected %s for path %s", grofs_node_type_to_str(node->type), path);
    }

    free(node);

    return FUSE_ERR(ret);
}

static int grofs_opendir(const char *path, struct fuse_file_info *file_info) {
    struct grofs_node *node;

    int ret = grofs_resolve_node_for_path(&node, path);

    if (0 != ret) {
        return FUSE_ERR(ret);
    }

    if (DATA == node->type) {
        free(node);

        return FUSE_ERR(ENOTDIR);
    }

    struct grofs_dir_handle *dir_handle;

    ret = grofs_opendir_create_dir_handle_from_node(&dir_handle, node);

    free(node);

    if (0 == ret) {
        file_info->fh = (uint64_t) dir_handle;
    }

    return FUSE_ERR(ret);
}

static int grofs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *file_info) {
    (void) path;

    struct grofs_dir_handle *dir_handle = (struct grofs_dir_handle *) file_info->fh;

    if (dir_handle->last_offset != offset) {
        return FUSE_ERR(EBADF);
    }

    struct grofs_buff *buff = &dir_handle->buff;

    int ret = 0;

    do {
        if (grofs_fill_from_dir_handle(dir_handle, buffer, filler)) {
            return 0;
        }

        grofs_buff_align_to_start(buff);

        ret = grofs_buff_realloc(buff);

        if (ret) {
            return FUSE_ERR(ret);
        }

        ret = read(dir_handle->fd, buff->data + buff->len, sizeof(char) * GROFS_READDIR_BUFF_LEN);

        if (ret < 0) {
            return FUSE_ERR(errno);
        }

        buff->len += ret;
    } while(ret > 0);

    return 0;
}

static int grofs_releasedir(const char *path, struct fuse_file_info *file_info) {
    (void) path;

    struct grofs_dir_handle *dir_handle = (struct grofs_dir_handle *) file_info->fh;

    if (NULL != dir_handle->thread_data.iter_payload) {
        free(dir_handle->thread_data.iter_payload);
    }

    if (NULL != dir_handle->buff.data) {
        free(dir_handle->buff.data);
    }

    grofs_releasedir_close_thread(dir_handle->fd, dir_handle->read_thr, &dir_handle->thread_data);

    close(dir_handle->fd);

    free(dir_handle);

    return 0;
}

static int grofs_open(const char *path, struct fuse_file_info *file_info) {
    if (file_info->flags & (O_WRONLY | O_RDWR)) {
        return FUSE_ERR(EROFS);
    }

    struct grofs_node *node;

    if (grofs_resolve_node_for_path(&node, path) != 0) {
        return FUSE_ERR(ENOENT);
    }

    int ret = FUSE_ERR(ENOENT);

    if (DIR == node->type) {
        ret = FUSE_ERR(EISDIR);
    } else if (DATA == node->type) {
        ret = grofs_open_node(node, file_info);
    }

    free(node);

    return FUSE_ERR(ret);
}

static int grofs_read(const char *path, char *buff, size_t size, off_t offset, struct fuse_file_info *file_info) {
    (void) path;

    struct grofs_file_handle *file_handle = (struct grofs_file_handle *) file_info->fh;

    if (offset >= file_handle->len) {
        return 0;
    }

    int to_read = grofs_min(size, file_handle->len - offset);

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
    grofs_started_time = time(NULL);

    git_libgit2_init();

    atexit(grofs_cleanup_on_exit_cb);

    grofs_args.argc = argc;
    grofs_args.argv = argv;
    grofs_args.allocated = 0;

    struct grofs_cli_opts cli_opts = {
        .show_version = 0,
        .show_help = 0
    };

    if(fuse_opt_parse(&grofs_args, &cli_opts, grofs_fuse_opts, grofs_fuse_args_process_cb) == -1) {
        fprintf(stderr, "Failed to parse options");

        return 1;
    }

    if (cli_opts.show_help) {
        grofs_print_help(grofs_args.argv[0]);

        fuse_opt_add_arg(&grofs_args, "-ho");

        return fuse_main(grofs_args.argc, grofs_args.argv, &grofs_fuse_operations, NULL);
    }

    if (cli_opts.show_version) {
        fprintf(stderr, "grofs version: %s\n", GROFS_VERSION);

        fuse_opt_add_arg(&grofs_args, "--version");

        return fuse_main(grofs_args.argc, grofs_args.argv, &grofs_fuse_operations, NULL);
    }

    if (NULL == grofs_repo_path) {
        fprintf(stderr, "Git repository path not provided\n\n");

        grofs_print_help(grofs_args.argv[0]);

        return 1;
    }

    int error = git_repository_open(&grofs_repo, grofs_repo_path);

    if (0 != error) {
        fprintf(stderr, "Failed to find Git repository at path: %s\n", grofs_repo_path);

        return 1;
    }

    return fuse_main(grofs_args.argc, grofs_args.argv, &grofs_fuse_operations, NULL);
}
