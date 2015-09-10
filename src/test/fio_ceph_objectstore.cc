/*
 *  Ceph ObjectStore engine
 *
 * IO engine using Ceph's ObjectStore class to test low-level performance of
 * Ceph OSDs.
 *
 */

#include "os/ObjectStore.h"
#include "global/global_init.h"

#include <fio.h>

struct fio_ceph_os_iou {
	struct io_u *io_u;
	int io_complete;
};

struct ceph_os_data {
	struct io_u **aio_events;
	ObjectStore *fs;
};

struct ceph_os_options {
	struct thread_data *td;
	char *objectstore;
	char *filestore_debug;
	char *filestore_journal;
};

// initialize the options in a function because g++ reports:
//   sorry, unimplemented: non-trivial designated initializers not supported
static struct fio_option* init_options() {
	static struct fio_option options[] = {{},{},{},{}};

	options[0].name     = "objectstore";
	options[0].lname    = "ceph objectstore type";
	options[0].type     = FIO_OPT_STR_STORE;
	options[0].help     = "Type of ObjectStore to create";
	options[0].off1     = offsetof(struct ceph_os_options, objectstore);
	options[0].def      = "filestore";
	options[0].category = FIO_OPT_C_ENGINE;
	options[0].group    = FIO_OPT_G_RBD;

	options[1].name     = "filestore_debug";
	options[1].lname    = "ceph filestore debug level";
	options[1].type     = FIO_OPT_STR_STORE;
	options[1].help     = "Debug level for ceph filestore log output";
	options[1].off1     = offsetof(struct ceph_os_options, filestore_debug);
	options[1].category = FIO_OPT_C_ENGINE;
	options[1].group    = FIO_OPT_G_RBD;

	options[2].name     = "filestore_journal";
	options[2].lname    = "ceph filestore journal path";
	options[2].type     = FIO_OPT_STR_STORE;
	options[2].help     = "Path for a temporary journal file";
	options[2].off1     = offsetof(struct ceph_os_options, filestore_journal);
	options[2].def      = "";
	options[2].category = FIO_OPT_C_ENGINE;
	options[2].group    = FIO_OPT_G_RBD;

	return options;
};

/////////////////////////////


struct OnCommitted : public Context {
  struct io_u *io_u;
  OnCommitted(struct io_u* io_u) : io_u(io_u) {}
  void finish(int r) {
  }
};

struct OnApplied : public Context {
  struct io_u *io_u;
  ObjectStore::Transaction *t;
  OnApplied(struct io_u* io_u, ObjectStore::Transaction *t) : io_u(io_u), t(t) {}
  void finish(int r) {

	struct fio_ceph_os_iou *fio_ceph_os_iou =
	    (struct fio_ceph_os_iou *)io_u->engine_data;

	fio_ceph_os_iou->io_complete = 1;


	delete t;
  }
};




static int _fio_setup_ceph_os_data(struct thread_data *td,
			       struct ceph_os_data **ceph_os_data_ptr)
{
	struct ceph_os_data *ceph_os_data;

	if (td->io_ops->data)
		return 0;

	ceph_os_data = (struct ceph_os_data*) malloc(sizeof(struct ceph_os_data));
	if (!ceph_os_data)
		goto failed;

	memset(ceph_os_data, 0, sizeof(struct ceph_os_data));

	ceph_os_data->aio_events = (struct io_u **) malloc(td->o.iodepth * sizeof(struct io_u *));
	if (!ceph_os_data->aio_events)
		goto failed;

	memset(ceph_os_data->aio_events, 0, td->o.iodepth * sizeof(struct io_u *));

	*ceph_os_data_ptr = ceph_os_data;

	return 0;

failed:
	return 1;

}

static struct io_u *fio_ceph_os_event(struct thread_data *td, int event)
{
	struct ceph_os_data *ceph_os_data = (struct ceph_os_data *) td->io_ops->data;

	return ceph_os_data->aio_events[event];
}

static int fio_ceph_os_getevents(struct thread_data *td, unsigned int min,
			     unsigned int max, const struct timespec *t)
{
	struct ceph_os_data *ceph_os_data = (struct ceph_os_data *) td->io_ops->data;
	unsigned int events = 0;
	struct io_u *io_u;
	unsigned int i;
	struct fio_ceph_os_iou *fov;

	do {
		io_u_qiter(&td->io_u_all, io_u, i) {
			if (!(io_u->flags & IO_U_F_FLIGHT))
				continue;

			fov = (struct fio_ceph_os_iou *)io_u->engine_data;

			if (fov->io_complete) {
				fov->io_complete = 0;
				ceph_os_data->aio_events[events] = io_u;
				events++;
			}

		}
		if (events < min)
			usleep(100);
		else
			break;

	} while (1);

	return events;
}

static int fio_ceph_os_queue(struct thread_data *td, struct io_u *io_u)
{
	int r = -1;
	char buf[32];
	struct ceph_os_data *ceph_os_data = (struct ceph_os_data *) td->io_ops->data;
	uint64_t len = io_u->xfer_buflen;
	uint64_t off = io_u->offset;
	ObjectStore *fs = ceph_os_data->fs;
	snprintf(buf, sizeof(buf), "XXX_%lu_%lu", io_u->start_time.tv_usec, io_u->start_time.tv_sec);
	spg_t pg;
	ghobject_t oid(pg.make_temp_object(buf));

	fio_ro_check(td, io_u);

	bufferlist data;
	data.push_back(buffer::create_static(len, (char *)io_u->xfer_buf));

        if (io_u->ddir == DDIR_WRITE) {
		ObjectStore::Transaction *t = new ObjectStore::Transaction;
		if (!t) {
			cout << "ObjectStore Transcation allocation failed." << std::endl;
			goto failed;
		}
		t->write(coll_t(pg), oid, off, len, data);
		//cout << "QUEUING transaction " << io_u << std::endl;
		fs->queue_transaction(NULL, t, new OnApplied(io_u, t), new OnCommitted(io_u));
	} else if (io_u->ddir == DDIR_READ) {
		r = fs->read(coll_t(pg), oid, off, len, data);
		if (r < 0)
			goto failed;
		io_u->resid = len - r;
		return FIO_Q_COMPLETED;
	} else {
		cout << "WARNING: No DDIR beside DDIR_WRITE supported!" << std::endl;
		return FIO_Q_COMPLETED;
	}

	return FIO_Q_QUEUED;

failed:
	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_ceph_os_init(struct thread_data *td)
{
	vector<const char*> args;
	struct ceph_os_data *ceph_os_data = (struct ceph_os_data *) td->io_ops->data;
	struct ceph_os_options *o = (struct ceph_os_options *) td->eo;
	ObjectStore::Transaction ft;

	global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, 0);
	//g_conf->journal_dio = false;
	common_init_finish(g_ceph_context);
	if (o->filestore_debug) {
		g_ceph_context->_conf->set_val("debug_filestore", o->filestore_debug);
		g_ceph_context->_conf->apply_changes(NULL);
	}

  ObjectStore *fs = ObjectStore::create(g_ceph_context,
			o->objectstore, td->o.directory,
			o->filestore_journal ? o->filestore_journal : "");
	if (fs == NULL) {
		cout << "bad objectstore type " << o->objectstore << std::endl;
		return 1;
	}
	if (fs->mkfs() < 0) {
		cout << "mkfs failed" << std::endl;
		return 1;
	}
	if (fs->mount() < 0) {
		cout << "mount failed" << std::endl;
		return 1;
	}

	spg_t pg;
	ft.create_collection(coll_t(pg), 0);
	fs->apply_transaction(ft);

	ceph_os_data->fs = fs;
	return 0;
}

static void fio_ceph_os_cleanup(struct thread_data *td)
{
	struct ceph_os_data *ceph_os_data = (struct ceph_os_data *) td->io_ops->data;

	if (ceph_os_data) {
		if (ceph_os_data->fs) {
			// clean up ObjectStore
			ceph_os_data->fs->umount();
			delete ceph_os_data->fs;
		}
		free(ceph_os_data->aio_events);
		free(ceph_os_data);
	}

}

static int fio_ceph_os_setup(struct thread_data *td)
{
	int r = 0;
	struct fio_file *f;
	struct ceph_os_data *ceph_os_data = NULL;

	/* allocate engine specific structure to deal with libceph_os. */
	r = _fio_setup_ceph_os_data(td, &ceph_os_data);
	if (r) {
		log_err("fio_setup_ceph_os_data failed.\n");
		goto cleanup;
	}
	td->io_ops->data = ceph_os_data;

	/* taken from "net" engine. Pretend we deal with files,
	 * even if we do not have any ideas about files.
	 * The size of the ObjectStore is set instead of a artificial file.
	 */
	if (!td->files_index) {
		add_file(td, td->o.filename ? : "ceph_os", 0, 0);
		td->o.nr_files = td->o.nr_files ? : 1;
	}
	f = td->files[0];
	f->real_file_size = 1024 * 1024; 

	return 0;

cleanup:
	fio_ceph_os_cleanup(td);
	return r;
}

static int fio_ceph_os_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static void fio_ceph_os_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct fio_ceph_os_iou *o = (struct fio_ceph_os_iou *) io_u->engine_data;

	if (o) {
		io_u->engine_data = NULL;
		free(o);
	}
}

static int fio_ceph_os_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct fio_ceph_os_iou *o;

	o = (struct fio_ceph_os_iou *) malloc(sizeof(*o));
	o->io_complete = 0;
	o->io_u = io_u;
	io_u->engine_data = o;
	return 0;
}

extern "C" {
void get_ioengine(struct ioengine_ops **ioengine_ptr) {
	struct ioengine_ops *ioengine;
	*ioengine_ptr = (struct ioengine_ops *) calloc(sizeof(struct ioengine_ops), 1);
	ioengine = *ioengine_ptr;

	strcpy(ioengine->name, "cephobjectstore");
	ioengine->version        = FIO_IOOPS_VERSION;
	ioengine->setup          = fio_ceph_os_setup;
	ioengine->init           = fio_ceph_os_init;
	//ioengine->prep           = fio_ceph_os_prep;
	ioengine->queue          = fio_ceph_os_queue;
	//ioengine->cancel         = fio_ceph_os_cancel;
	ioengine->getevents      = fio_ceph_os_getevents;
	ioengine->event          = fio_ceph_os_event;
	ioengine->cleanup        = fio_ceph_os_cleanup;
	ioengine->open_file      = fio_ceph_os_open;
	//ioengine->close_file     = fio_ceph_os_close;
	ioengine->io_u_init      = fio_ceph_os_io_u_init;
	ioengine->io_u_free      = fio_ceph_os_io_u_free;
	ioengine->options        = init_options();
	ioengine->option_struct_size = sizeof(struct ceph_os_options);
}
}

