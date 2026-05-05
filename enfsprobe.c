/*
 * enfsprobe — quick NFS export health check using libnfs (userspace client).
 *
 * Prints "OK" on stdout on success. Diagnostics on stderr.
 * Exit: 0 ok, 1 probe failed, 2 usage/invalid args, 3 watchdog timeout.
 */

#define _GNU_SOURCE

#include <ctype.h>
#include <dlfcn.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/*
 * Include path depends on how libnfs is installed:
 *   pkg-config Cflags with -I…/nfsc  →  #include <libnfs.h>
 *   else (no .pc, or .pc without Cflags)  →  #include <nfsc/libnfs.h>
 * Makefile sets -DENFSPROBE_NFSC_LIBNFS_H=1 for the latter.
 */
#ifdef ENFSPROBE_NFSC_LIBNFS_H
#  include <nfsc/libnfs.h>
#else
#  include <libnfs.h>
#endif

typedef int (*enfsprobe_nfs_set_version_fn)(struct nfs_context *, int);

/*
 * nfs_set_version() exists only in newer libnfs; EL7 links without it.
 * Resolve at runtime so one binary builds everywhere.
 */
static enfsprobe_nfs_set_version_fn
enfsprobe_resolve_nfs_set_version(void)
{
	static enfsprobe_nfs_set_version_fn fn;
	static int done;

	if (done)
		return fn;
	done = 1;
	fn = (enfsprobe_nfs_set_version_fn)(void *)dlsym(RTLD_DEFAULT,
	    "nfs_set_version");
	return fn;
}

static void
usage(FILE *fp)
{
	fprintf(fp,
	    "usage: enfsprobe <mount> <timeout-seconds>\n"
	    "\n"
	    "  mount:   nfs://host/path[?opts]   or   host:/path[?opts]\n"
	    "           Use nfs://host/path — not nfs://host:/path (extra ':' breaks libnfs).\n"
	    "           Default NFS version is v4 (?version=4) unless you set version= in ?opts.\n"
	    "  timeout: hard wall-clock limit for the probe (watchdog), >= 1.\n"
	    "\n"
	    "Stdout: OK on success. Stderr: progress and errors.\n");
}

static char *
trim_dup(const char *s)
{
	size_t n = strlen(s);
	while (n > 0 && isspace((unsigned char)s[n - 1]))
		n--;
	while (*s && isspace((unsigned char)*s))
		s++, n--;
	if (n == 0)
		return NULL;
	char *out = malloc(n + 1);
	if (!out)
		return NULL;
	memcpy(out, s, n);
	out[n] = '\0';
	return out;
}

static bool
parse_positive_int(const char *s, int *out)
{
	char *end = NULL;
	errno = 0;
	long v = strtol(s, &end, 10);
	if (errno || !end || *end != '\0' || v < 1 || v > 86400)
		return false;
	*out = (int)v;
	return true;
}

static bool
query_has_version_key(const char *q)
{
	if (!q || !*q)
		return false;
	if (strncmp(q, "version=", 8) == 0)
		return true;
	return strstr(q, "&version=") != NULL;
}

/*
 * nfs://host:/path and nfs://host:/?query are common typos (merging host:/… with nfs://).
 * libnfs wants nfs://host/path?… — drop the stray ':' before the first '/' or '?'.
 * Also nfs://host:?q → nfs://host/?q. Do not touch nfs://host:2049/path (port).
 */
static char *
normalize_leading_nfs_url(const char *spec)
{
	if (strncmp(spec, "nfs://", 6) != 0)
		return strdup(spec);

	const char *p = spec + 6;
	size_t skip = strcspn(p, "/?");
	const char *sep = p + skip;

	if (sep <= p || sep[-1] != ':')
		return strdup(spec);
	if (*sep != '/' && *sep != '?' && *sep != '\0')
		return strdup(spec);

	size_t hostlen = (size_t)(sep - 1 - p);

	if (*sep == '?') {
		size_t qlen = strlen(sep);
		char *out = malloc(6 + hostlen + 1 + qlen + 1);
		if (!out)
			return NULL;
		memcpy(out, "nfs://", 6);
		memcpy(out + 6, p, hostlen);
		out[6 + hostlen] = '/';
		memcpy(out + 6 + hostlen + 1, sep, qlen + 1);
		return out;
	}

	if (*sep == '\0') {
		char *out = malloc(6 + hostlen + 2);
		if (!out)
			return NULL;
		memcpy(out, "nfs://", 6);
		memcpy(out + 6, p, hostlen);
		memcpy(out + 6 + hostlen, "/", 2);
		return out;
	}

	const char *rest = sep;
	size_t restlen = strlen(rest);
	char *out = malloc(6 + hostlen + restlen + 1);
	if (!out)
		return NULL;
	memcpy(out, "nfs://", 6);
	memcpy(out + 6, p, hostlen);
	memcpy(out + 6 + hostlen, rest, restlen + 1);
	return out;
}

static char *
nfs_url_ensure_version_4(const char *url)
{
	const char *q = strchr(url, '?');
	size_t base_len;
	const char *query;

	if (q != NULL) {
		base_len = (size_t)(q - url);
		query = q + 1;
	} else {
		base_len = strlen(url);
		query = NULL;
	}

	if (query_has_version_key(query))
		return strdup(url);

	char query_final[512];
	if (query == NULL || query[0] == '\0') {
		if (snprintf(query_final, sizeof(query_final), "version=4") >= (int)sizeof(query_final))
			return NULL;
	} else {
		if (snprintf(query_final, sizeof(query_final), "%s&version=4",
			query) >= (int)sizeof(query_final))
			return NULL;
	}

	size_t qflen = strlen(query_final);
	char *out = malloc(base_len + 1 + qflen + 1);
	if (!out)
		return NULL;
	memcpy(out, url, base_len);
	out[base_len] = '?';
	memcpy(out + base_len + 1, query_final, qflen + 1);
	return out;
}

/*
 * Traditional NFS spec host:/path[?query] → nfs URL.
 * Appends version=4 only when the tail query does not already set version=.
 * (Previously appending ?version=4 after "/?version=4" produced an invalid URL
 * and libnfs could fall back to NFSv3 mount — MNT3ERR_* errors.)
 */
static char *
spec_to_url(const char *spec)
{
	if (strncmp(spec, "nfs://", 6) == 0) {
		char *norm = normalize_leading_nfs_url(spec);
		if (!norm)
			return NULL;
		char *withver = nfs_url_ensure_version_4(norm);
		free(norm);
		return withver;
	}

	const char *colon = strchr(spec, ':');
	if (!colon || colon[1] != '/')
		return NULL;

	size_t hostlen = (size_t)(colon - spec);
	const char *path = colon + 1;
	if (hostlen == 0)
		return NULL;

	if (*path == '\0')
		path = "/";
	else if (*path != '/')
		return NULL;

	const char *qmark = strchr(path, '?');
	size_t path_only_len;
	const char *user_query;

	if (qmark != NULL) {
		path_only_len = (size_t)(qmark - path);
		user_query = qmark + 1;
		if (path_only_len == 0)
			return NULL;
	} else {
		path_only_len = strlen(path);
		user_query = NULL;
	}

	char query_final[512];
	if (user_query == NULL || user_query[0] == '\0') {
		if (snprintf(query_final, sizeof(query_final), "version=4") >= (int)sizeof(query_final))
			return NULL;
	} else if (query_has_version_key(user_query)) {
		if (snprintf(query_final, sizeof(query_final), "%s", user_query) >= (int)sizeof(query_final))
			return NULL;
	} else {
		if (snprintf(query_final, sizeof(query_final), "%s&version=4",
			user_query) >= (int)sizeof(query_final))
			return NULL;
	}

	size_t qflen = strlen(query_final);
	size_t need = 6 + hostlen + path_only_len + 1 + qflen + 1;
	char *url = malloc(need);
	if (!url)
		return NULL;

	memcpy(url, "nfs://", 6);
	memcpy(url + 6, spec, hostlen);
	memcpy(url + 6 + hostlen, path, path_only_len);
	url[6 + hostlen + path_only_len] = '?';
	memcpy(url + 6 + hostlen + path_only_len + 1, query_final, qflen + 1);
	return url;
}

/*
 * libnfs on some older systems does not apply version= from the URL; default v4.
 */
static int
nfs_major_version_from_urlbuf(const char *url)
{
	const char *q = strchr(url, '?');
	const char *scan = q != NULL ? q + 1 : url;

	if (strstr(scan, "version=3") != NULL)
		return 3;
	if (strstr(scan, "version=4") != NULL)
		return 4;
	return 4;
}

static int
probe(const char *spec, int timeout_sec)
{
	char *urlbuf = spec_to_url(spec);
	if (!urlbuf) {
		fprintf(stderr,
		    "enfsprobe: invalid mount spec %s (expected host:/path or nfs://…)\n",
		    spec);
		return 2;
	}

	int want_nfs_ver = nfs_major_version_from_urlbuf(urlbuf);

	struct nfs_context *nfs = nfs_init_context();
	if (!nfs) {
		fprintf(stderr, "enfsprobe: nfs_init_context failed\n");
		free(urlbuf);
		return 1;
	}

	int rpc_ms = timeout_sec * 1000;
	if (rpc_ms < 1000)
		rpc_ms = 1000;

	nfs_set_timeout(nfs, rpc_ms);

	fprintf(stderr, "enfsprobe: probing %s …\n", urlbuf);

	struct nfs_url *url = nfs_parse_url_dir(nfs, urlbuf);
	free(urlbuf);
	urlbuf = NULL;
	if (!url) {
		fprintf(stderr, "enfsprobe: URL parse failed: %s\n",
		    nfs_get_error(nfs) ? nfs_get_error(nfs) : "(no detail)");
		nfs_destroy_context(nfs);
		return 1;
	}

	enfsprobe_nfs_set_version_fn set_ver = enfsprobe_resolve_nfs_set_version();
	if (set_ver != NULL) {
		if (set_ver(nfs, want_nfs_ver) != 0) {
			fprintf(stderr, "enfsprobe: nfs_set_version(%d) failed: %s\n",
			    want_nfs_ver,
			    nfs_get_error(nfs) ? nfs_get_error(nfs) : "(no detail)");
			nfs_destroy_url(url);
			nfs_destroy_context(nfs);
			return 1;
		}
		fprintf(stderr, "enfsprobe: using NFS protocol version %d\n",
		    want_nfs_ver);
	} else {
		fprintf(stderr,
		    "enfsprobe: warning: libnfs has no nfs_set_version(); "
		    "protocol may ignore ?version= (newer libnfs fixes this)\n");
	}

	int rc = 1;
	if (nfs_mount(nfs, url->server, url->path) < 0) {
		const char *err = nfs_get_error(nfs);
		fprintf(stderr, "enfsprobe: mount failed: %s\n",
		    err != NULL && err[0] != '\0' ? err : strerror(errno));
		goto done;
	}

	struct nfs_stat_64 st;
	memset(&st, 0, sizeof(st));
	if (nfs_stat64(nfs, ".", &st) < 0) {
		fprintf(stderr, "enfsprobe: getattr on export root failed: %s\n",
		    nfs_get_error(nfs) ? nfs_get_error(nfs) : strerror(errno));
		goto done;
	}

	fprintf(stderr,
	    "enfsprobe: OK — mounted and getattr(\".\") on export root succeeded\n");
	printf("OK\n");
	fflush(stdout);
	rc = 0;

done:
	nfs_destroy_url(url);
	nfs_destroy_context(nfs);
	return rc;
}

static int
wait_timeout(pid_t pid, int watchdog_sec)
{
	struct timespec start, now;
	if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
		perror("enfsprobe: clock_gettime");
		return -1;
	}

	for (;;) {
		int status = 0;
		pid_t r = waitpid(pid, &status, WNOHANG);
		if (r == pid) {
			if (WIFEXITED(status))
				return WEXITSTATUS(status);
			if (WIFSIGNALED(status)) {
				fprintf(stderr,
				    "enfsprobe: child terminated by signal %d\n",
				    WTERMSIG(status));
				return 1;
			}
			return 1;
		}
		if (r < 0) {
			perror("enfsprobe: waitpid");
			return -1;
		}

		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
			perror("enfsprobe: clock_gettime");
			return -1;
		}
		long elapsed = now.tv_sec - start.tv_sec;
		if (elapsed >= watchdog_sec) {
			kill(pid, SIGTERM);
			usleep(200000);
			kill(pid, SIGKILL);
			for (;;) {
				r = waitpid(pid, &status, 0);
				if (r == pid)
					break;
				if (r < 0 && errno != EINTR)
					break;
			}
			fprintf(stderr,
			    "enfsprobe: watchdog: no response within %ds\n",
			    watchdog_sec);
			return 3;
		}
		usleep(50000);
	}
}

int
main(int argc, char **argv)
{
	if (argc != 3) {
		usage(stderr);
		return 2;
	}

	char *spec = trim_dup(argv[1]);
	if (!spec) {
		fprintf(stderr, "enfsprobe: empty mount argument\n");
		return 2;
	}

	int timeout_sec = 0;
	if (!parse_positive_int(argv[2], &timeout_sec)) {
		fprintf(stderr, "enfsprobe: invalid timeout %s\n", argv[2]);
		free(spec);
		return 2;
	}

	int watchdog_sec = timeout_sec + 5;
	if (watchdog_sec < timeout_sec)
		watchdog_sec = timeout_sec;

	pid_t pid = fork();
	if (pid < 0) {
		perror("enfsprobe: fork");
		free(spec);
		return 2;
	}

	if (pid == 0) {
		int x = probe(spec, timeout_sec);
		free(spec);
		if (x > 255)
			x = 255;
		_exit(x);
	}

	free(spec);

	int final = wait_timeout(pid, watchdog_sec);
	if (final < 0)
		return 2;
	return final;
}
