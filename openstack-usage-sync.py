import argparse

import logging
import MySQLdb


LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())


def mysql_connect(host, user, password, db_name):
    return MySQLdb.connect(host=host,
                           user=user,
                           passwd=password,
                           db=db_name)


def dump_quota_usages(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute('SELECT id, project_id as project_id, '
                   'resource, in_use, updated_at '
                   'FROM quota_usages')
    quota_usages = cursor.fetchall()
    cursor.close()

    #NOTE(bcwaldon): Normalize reported usage info into a usable dict
    # that looks like this:
    # {
    #  <project_id>: {
    #      <resource>: (<amount_in_use>, <last_updated_datetime>),
    #      ...
    #  },
    #  ...,
    # }
    #
    norm_quota_usages = {}
    for qu in quota_usages:
        project_id = qu['project_id']
        norm_quota_usages.setdefault(project_id, {})
        norm_quota_usages[project_id][qu['resource']] = \
            (qu['in_use'], qu['id'], qu['updated_at'])

    return norm_quota_usages


def dump_cinder_usages(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute('SELECT project_id, '
                   'COUNT(size) as volumes, '
                   'SUM(size) as gigabytes '
                   'FROM volumes '
                   'WHERE deleted=0 '
                   'GROUP BY project_id')
    cinder_usages = cursor.fetchall()
    cinder_usages = dict((u.pop('project_id'), u) for u in cinder_usages)
    cursor.close()

    return cinder_usages


def dump_nova_usages(conn):
    nova_usages = {}

    def _update_nova_usages(moar):
        for record in moar:
            project_id = record.pop('project_id')
            try:
                nova_usages[project_id].update(record)
            except KeyError:
                nova_usages[project_id] = record

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute('SELECT project_id, '
                   'COUNT(vcpus) as instances, '
                   'SUM(vcpus) as cores, '
                   'SUM(memory_mb) as ram '
                   'FROM instances '
                   'WHERE deleted=0 '
                   'GROUP BY project_id')
    _update_nova_usages(cursor.fetchall())
    cursor.close()

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute('SELECT project_id, '
                   'COUNT(*) as floating_ips '
                   'FROM floating_ips '
                   'WHERE deleted=0 AND project_id is not NULL '
                   'GROUP BY project_id')
    _update_nova_usages(cursor.fetchall())
    cursor.close()

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute('SELECT project_id, '
                   'COUNT(*) as security_groups '
                   'FROM security_groups '
                   'WHERE deleted=0 AND project_id is not NULL '
                   'GROUP BY project_id')
    _update_nova_usages(cursor.fetchall())
    cursor.close()

    return nova_usages


def generate_diff(conn, quota_usages, actual_usages, resources):
    for (project_id, reported_project_usages) in quota_usages.items():
        actual_project_usages = actual_usages.get(project_id, {})
        for resource in resources:
            try:
                reported_in_use = reported_project_usages[resource][0]
            except KeyError:
                continue

            actual_in_use = actual_project_usages.get(resource, 0)

            LOG.info('USAGE tenant=%s resource=%s actual=%s reported=%s' \
                 % (project_id, resource, actual_in_use, reported_in_use))

            if actual_in_use != reported_in_use:
                quota_usage_id, updated_at = reported_project_usages[resource][1:3]
                yield (project_id, resource, actual_in_use,
                       quota_usage_id, updated_at)


def apply_update(conn, *record):
    (tenant, resource, actual, quota_usage_id, updated_at) = record
    LOG.info('UPDATE tenant=%s resource=%s in_use=%s'
             % (tenant, resource, actual))
    cursor = conn.cursor()
    query = ('UPDATE quota_usages SET in_use=%s '
             'WHERE id=%s AND updated_at=\'%s\'') \
            % (actual, quota_usage_id, updated_at)
    LOG.debug('SQL: %s' % query)
    cursor.execute(query)
    cursor.close()
    conn.commit()


def sync(mysql_host, mysql_user, mysql_password, dry_run=False):
    databases = {
        'cinder': (dump_cinder_usages, ['volumes', 'gigabytes']),
        'nova': (dump_nova_usages,
                 ['instances', 'cores', 'ram', 'floating_ips',
                  'security_groups'],
        ),
    }

    for (db, (dump_func, resources)) in databases.items():
        LOG.debug('Syncing database=%s resources=%s' % (db, resources))
        conn = mysql_connect(mysql_host, mysql_user, mysql_password, db)

        quota_usages = dump_quota_usages(conn)
        actual_usages = dump_func(conn)
        diff = generate_diff(conn, quota_usages, actual_usages, resources)

        #TODO(bcwaldon): have to iterate through the generator to get the
        # usage info logged, maybe we can fix this later
        for change in diff:
            if not dry_run:
                apply_update(conn, *change)

        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Sync usage info within OpenStack databases')
    parser.add_argument('--host', default='localhost',
                        help='MySQL host.')
    parser.add_argument('-u', '--user', default='root',
                        help='MySQL username.')
    parser.add_argument('-p', '--password', default=None,
                        help='MySQL password.')
    parser.add_argument('--dry-run', default=False, action='store_true',
                        help='Prevent any actual changes from being made.')
    parser.add_argument('-d', '--debug', action='store_true', default=False,
                        help='Log debug output.')
    args = parser.parse_args()

    LOG.setLevel(logging.DEBUG if args.debug else logging.INFO)

    sync(args.host, args.user, args.password, args.dry_run)
