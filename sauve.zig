const std = @import("std");

const ctime = @cImport({
    @cDefine("_XOPEN_SOURCE", {});
    @cDefine("_DEFAULT_SOURCE", {});
    @cInclude("time.h");
});

const application_name = "sauve";

const alloc = std.heap.c_allocator;

const rfc_3339_format_string = "%Y-%m-%dT%H:%M:%SZ";

const XdgDir = struct {
    env_var: [:0]const u8,
    default_path: [:0]const u8,
};

const xdg_config_dir = XdgDir{
    .env_var = "XDG_CONFIG_DIR",
    .default_path = ".config",
};

const xdg_state_dir = XdgDir{
    .env_var = "XDG_STATE_DIR",
    .default_path = ".local/state",
};

fn xdgHelper(dir: XdgDir, relative_path: [:0]const u8) ![:0]const u8 {
    const s = (if (std.c.getenv(dir.env_var)) |explicit_dir|
        &[_][:0]const u8{ std.mem.span(explicit_dir), application_name, relative_path }
    else
        &[_][:0]const u8{
            std.mem.span(std.c.getenv("HOME") orelse unreachable),
            dir.default_path,
            application_name,
            relative_path,
        });

    const x = try std.mem.joinZ(alloc, "/", s);

    return x;
}

fn get_device_config_file_name() ![:0]const u8 {
    return xdgHelper(xdg_config_dir, "devices.txt");
}

fn get_config_devices_text() ![]const u8 {
    const path = try get_device_config_file_name();

    const fd = blk: {
        defer alloc.free(path);

        break :blk try std.posix.openZ(path, std.posix.O{ .ACCMODE = std.posix.ACCMODE.RDONLY, .CLOEXEC = true }, 0o644);
    };

    const stat = try std.posix.fstat(fd);

    const size: usize = @intCast(stat.size);

    const p = try std.posix.mmap(null, size, std.os.linux.PROT.READ, std.os.linux.MAP{ .TYPE = std.os.linux.MAP_TYPE.PRIVATE }, fd, 0);

    return p;
}

const Device = struct {
    label: []const u8,
    udisks_info: ?UDisksInfo,
    pos: usize,
    timerfd: std.posix.fd_t,

    state: State,

    const State = union(enum) {
        waited_for: PlugInRequest,
        in_use: BackupProcess,
        up_to_date,
    };
};

const PlugInRequest = struct {
    notif_id: NotificationId,
};

const BackupProcess = struct {
    proc: std.process.Child,
    pid_fd: std.posix.fd_t,
    notif: NotificationId,
};

const UDisksInfo = struct {
    mount_path: ?[]const u8,
    slot: *sd_bus.sd_bus_slot,
};

const mount_points_property = "MountPoints";

fn readMountPointMessage(m: *sd_bus.sd_bus_message, dev: *Device) !void {
    {
        const res = sd_bus.sd_bus_message_enter_container(m, 'v', "aay");
        std.debug.assert(0 < res);
    }

    {
        const res = sd_bus.sd_bus_message_enter_container(m, 'a', "ay");
        std.debug.assert(0 < res);
    }

    const path = try read_opt_byte_array(m);

    dev.udisks_info.?.mount_path = path;

    if (path) |x| {
        std.debug.print("There's a mount point: {s}\n", .{x});

        switch (dev.state) {
            Device.State.waited_for => |req| {
                _ = req;
                try initiateBackup(dev);
            },
            Device.State.up_to_date => {},
            else => unreachable,
        }
    } else {
        std.debug.print("There's no mount point\n", .{});
    }

    // We crash if we get more than one mount point, that would be nonsense.
    {
        const res = sd_bus.sd_bus_message_exit_container(m);
        std.debug.assert(res == 1);
    }
}

fn propertiesChangedCallback(
    m: ?*sd_bus.sd_bus_message,
    d: ?*anyopaque,
    err: [*c]sd_bus.sd_bus_error,
) callconv(.C) c_int {
    _ = err;

    // The UDisks service never uses the invalidated_properties field of the signal, and always sends the new
    // value, at least for mount points of filesystems.

    const state: *Device = @alignCast(@ptrCast(d));

    if (state.udisks_info) |*inf| {
        {
            var interface_name: [*:0]const u8 = undefined;
            const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&interface_name));
            std.debug.assert(0 < res);
            if (std.mem.orderZ(u8, interface_name, interface_of_interest) != std.math.Order.eq) {
                return 0;
            }
        }

        {
            const res = sd_bus.sd_bus_message_enter_container(m, 'a', "{sv}");
            std.debug.assert(0 < res);
        }

        while (true) {
            {
                const res = sd_bus.sd_bus_message_enter_container(m, 'e', "sv");
                std.debug.assert(0 <= res);
                if (res == 0) {
                    break;
                }
            }

            const property_name = blk: {
                var property_name: [*:0]const u8 = undefined;
                const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&property_name));
                std.debug.assert(0 < res);
                break :blk property_name;
            };

            if (std.mem.orderZ(u8, property_name, mount_points_property) == std.math.Order.eq) {
                if (inf.mount_path) |x| {
                    alloc.free(x);
                }

                readMountPointMessage(m.?, state) catch unreachable;

                return 0;
            }

            {
                const res = sd_bus.sd_bus_message_exit_container(m);
                std.debug.assert(res == 1);
            }
        }
    } else {
        std.debug.print("Ignoring signal.\n", .{});
    }

    return 0;
}

fn read_opt_byte_array(m: *sd_bus.sd_bus_message) !?[]const u8 {
    {
        const res = sd_bus.sd_bus_message_enter_container(m, 'a', "y");
        std.debug.assert(0 <= res);
        if (res == 0) {
            return null;
        }
    }

    var ret = std.ArrayList(u8).init(alloc);

    while (true) {
        var byte: u8 = undefined;

        const res = sd_bus.sd_bus_message_read_basic(m, 'y', @ptrCast(&byte));
        std.debug.assert(0 <= res);
        if (res == 0) {
            break;
        }

        try ret.append(byte);
    }

    {
        const res = sd_bus.sd_bus_message_exit_container(m);
        std.debug.assert(res == 1);
    }

    return try ret.toOwnedSlice();
}

fn start_watching_device(
    bus: *sd_bus.sd_bus,
    object_path: [*:0]const u8,
    state: *Device,
) !void {
    const slot = blk: {
        var slot: ?*sd_bus.sd_bus_slot = undefined;
        const res = sd_bus.sd_bus_match_signal(
            bus,
            &slot,
            udisks_bus_name,
            object_path,
            properties_interface,
            "PropertiesChanged",
            propertiesChangedCallback,
            @ptrCast(state),
        );
        std.debug.assert(0 < res);
        break :blk slot.?;
    };

    const reply = call: {
        const request = blk: {
            var request: ?*sd_bus.sd_bus_message = undefined;
            const res = sd_bus.sd_bus_message_new_method_call(
                bus,
                &request,
                udisks_bus_name,
                object_path,
                properties_interface,
                "Get",
            );
            std.debug.assert(res <= 0);
            break :blk request;
        };

        defer {
            const res = sd_bus.sd_bus_message_unref(request);
            std.debug.assert(res == null);
        }

        {
            const res = sd_bus.sd_bus_message_append_basic(
                request,
                's',
                @ptrCast(filesystem_interface),
            );
            std.debug.assert(0 <= res);
        }
        {
            const res = sd_bus.sd_bus_message_append_basic(
                request,
                's',
                @ptrCast(mount_points_property),
            );
            std.debug.assert(0 <= res);
        }

        {
            var err = null_error;
            var reply: ?*sd_bus.sd_bus_message = undefined;
            const res = sd_bus.sd_bus_call(bus, request, 0, &err, &reply);
            std.debug.assert(0 <= res);
            break :call reply.?;
        }
    };

    defer {
        const res = sd_bus.sd_bus_message_unref(reply);
        std.debug.assert(res == null);
    }

    state.udisks_info = UDisksInfo{ .slot = slot, .mount_path = null };
    try readMountPointMessage(reply, state);
}

const null_error = dbus_bindings.null_error;

const StrIndex = std.StringHashMap(*Device);

fn getConfigDevices() !struct { map: StrIndex, devices: []Device } {
    const devs = blk: {
        const text = try get_config_devices_text();

        var alist = std.ArrayList(Device).init(alloc);

        var it = std.mem.splitSequence(u8, text, "\n");

        // This ad hoc way of iterating is there to deal with the trailing newline
        // in the file.
        var line = it.next().?;
        while (true) {
            const x = it.next() orelse break;

            const state = Device{
                .label = line,
                .udisks_info = null,
                .pos = alist.items.len,
                .timerfd = try std.posix.timerfd_create(
                    std.os.linux.CLOCK.REALTIME,
                    std.os.linux.TFD{ .CLOEXEC = true },
                ),
                .state = Device.State{ .up_to_date = {} },
            };

            try alist.append(state);
            line = x;
        }

        break :blk try alist.toOwnedSlice();
    };

    const map = blk: {
        var map = StrIndex.init(alloc);
        for (devs) |*p| {
            const last_caltime = getLastBackupCaltime(p.label);
            if (last_caltime) |calt| {
                const next_calt = nextCaltime(calt);
                try armTimer(next_calt, p);
            } else {
                try requestDevice(p);
            }

            try map.putNoClobber(p.label, p);
        }
        break :blk map;
    };

    return .{ .map = map, .devices = devs };
}

const caltime = ctime.struct_tm;

fn parse_caltime(s: [*c]const u8) caltime {
    var tm: ctime.struct_tm = undefined;

    const res = ctime.strptime(s, rfc_3339_format_string, &tm);

    std.debug.assert(res != null);

    return tm;
}

fn getLastBackupCaltime(label: []const u8) ?caltime {
    const statement_text = @embedFile("look-up-date.sql");

    var statement: ?*sqlite.sqlite3_stmt = undefined;

    {
        const res = sqlite.sqlite3_prepare_v2(
            database,
            statement_text,
            statement_text.len,
            &statement,
            null,
        );
        std.debug.assert(res == sqlite.SQLITE_OK);
    }
    defer _ = sqlite.sqlite3_finalize(statement);

    {
        const res = sqlite.sqlite3_bind_text(
            statement,
            1,
            label.ptr,
            @intCast(label.len),
            sqlite.SQLITE_STATIC,
        );
        std.debug.assert(res == sqlite.SQLITE_OK);
    }

    {
        const res = sqlite.sqlite3_step(statement);

        return switch (res) {
            sqlite.SQLITE_DONE => null,
            sqlite.SQLITE_ROW => {
                const caltime_str = sqlite.sqlite3_column_text(statement, 1);
                return parse_caltime(caltime_str);
            },
            else => unreachable,
        };
    }
}

/// Given the calendar time of a backup on a device, returns the calendar path
/// after which a new backup should be made to that device.
fn nextCaltime(calt: caltime) caltime {
    var x = calt;

    x.tm_mday += 4;

    x.tm_hour = 10;
    x.tm_min = 0;
    x.tm_sec = 0;

    _ = ctime.timegm(&x);

    return x;
}

fn getNow() !ctime.struct_tm {
    const clock_id = std.os.linux.CLOCK.REALTIME;

    var tp: std.posix.timespec = undefined;

    try std.posix.clock_gettime(clock_id, &tp);

    var tm: ctime.struct_tm = undefined;

    const res = ctime.gmtime_r(&tp.tv_sec, &tm);

    if (res == null) {
        const err = error.TimeConversionFailed;

        return err;
    }

    return tm;
}

const dbus_bindings = @import("raw_dbus_bindings.zig");

const sd_bus = dbus_bindings.sd_bus;

const NotificationId = u32;

const notif_bus_name = "org.gnome.Shell.Notifications";
const notif_object_path = "/org/freedesktop/Notifications";
const notif_interface_name = "org.freedesktop.Notifications";

fn sendNotification(
    summary: [:0]const u8,
    body: ?[:0]const u8,
    replaces_id: ?u32,
) NotificationId {
    const bus = blk: {
        var bus: ?*sd_bus.sd_bus = undefined;
        {
            const res = sd_bus.sd_bus_default_user(&bus);

            std.debug.assert(0 <= res);
        }
        break :blk bus;
    };
    defer std.debug.assert(sd_bus.sd_bus_unref(bus) == null);

    const summary_p: [*:0]const u8 = summary;
    const body_p: ?[*:0]const u8 = body orelse null;

    const inner_replaces_id = replaces_id orelse 0;

    const reply = blk: {
        var err: sd_bus.sd_bus_error = .{ .name = null, .message = null };
        var reply: ?*sd_bus.sd_bus_message = undefined;

        const res = sd_bus.sd_bus_call_method(
            bus,
            notif_bus_name,
            notif_object_path,
            notif_interface_name,
            "Notify",
            &err,
            &reply,
            "susssasa{sv}i",
            application_name,
            inner_replaces_id,
            "",
            summary_p,
            body_p,
            @as(i32, 0),
            @as(i32, 0),
            @as(i32, -1),
        );

        if (res < 0) {
            std.debug.print("error code: {}\n", .{res});

            std.debug.print("{s}\n", .{err.name});
            if (err.message) |m| {
                std.debug.print("{s}\n", .{m});
            }
        }
        std.debug.assert(0 <= res);

        break :blk reply;
    };
    defer {
        const res = sd_bus.sd_bus_message_unref(reply);
        std.debug.assert(res == null);
    }

    const id = blk: {
        var id: u32 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(reply, 'u', @ptrCast(&id));
        std.debug.assert(0 < res);

        break :blk id;
    };

    return id;
}

fn sendBackupNotification(mount_path: []const u8) !NotificationId {
    const msg_slice = blk: {
        const slices = &[_][]const u8{ "Performing backup to: ", mount_path };

        break :blk try std.mem.joinZ(alloc, "", slices);
    };
    defer alloc.free(msg_slice);

    const msg: [*:0]const u8 = msg_slice;

    sendNotification(msg);
}

fn closeNotification(id: NotificationId) void {
    const bus = blk: {
        var bus: ?*sd_bus.sd_bus = undefined;
        {
            const res = sd_bus.sd_bus_default_user(&bus);

            std.debug.assert(0 <= res);
        }
        break :blk bus;
    };
    defer std.debug.assert(sd_bus.sd_bus_unref(bus) == null);

    const request = blk: {
        var request: ?*sd_bus.sd_bus_message = undefined;
        const res = sd_bus.sd_bus_message_new_method_call(
            bus,
            &request,
            notif_bus_name,
            notif_object_path,
            notif_interface_name,
            "CloseNotification",
        );
        std.debug.assert(0 <= res);
        break :blk request;
    };

    {
        const res = sd_bus.sd_bus_message_append_basic(request, 'u', @ptrCast(&id));
        std.debug.assert(0 <= res);
    }

    {
        var err = null_error;
        const res = sd_bus.sd_bus_call(bus, request, 0, &err, null);
        if (res < 0) {
            std.debug.print("{s}\n", .{err.name});
            if (err.message) |m| {
                std.debug.print("{s}\n", .{m});
            }
        }
        std.debug.assert(0 <= res);
    }
}

fn logBackUpCalt(label: []const u8) !caltime {
    const db = database;

    const now = try getNow();

    const needed_size = ctime.strftime(null, std.math.maxInt(usize), rfc_3339_format_string, &now);

    const s = try alloc.allocSentinel(u8, needed_size, 0);
    defer alloc.free(s);

    const other_size = ctime.strftime(s, needed_size + 1, rfc_3339_format_string, &now);

    std.debug.assert(other_size == needed_size);

    const statement_text = @embedFile("log-backup.sql");

    var statement: ?*sqlite.sqlite3_stmt = undefined;

    {
        const res = sqlite.sqlite3_prepare_v2(db, statement_text, -1, &statement, null);
        std.debug.assert(res == sqlite.SQLITE_OK);
    }
    defer _ = sqlite.sqlite3_finalize(statement);

    {
        const res = sqlite.sqlite3_bind_text(
            statement,
            1,
            label.ptr,
            @intCast(label.len),
            sqlite.SQLITE_STATIC,
        );

        std.debug.assert(res == sqlite.SQLITE_OK);
    }

    {
        const res = sqlite.sqlite3_bind_text(statement, 2, s, -1, sqlite.SQLITE_STATIC);
        std.debug.assert(res == sqlite.SQLITE_OK);
    }

    {
        const res = sqlite.sqlite3_step(statement);
        std.debug.assert(res == sqlite.SQLITE_DONE);
    }

    return now;
}

fn get_timerfd() !std.posix.fd_t {
    const S = struct {
        var fd: ?std.posix.fd_t = undefined;
    };

    const fd = if (S.fd) |x| x else blk: {
        const x =
            try std.posix.timerfd_create(std.os.linux.CLOCK.REALTIME, std.os.linux.TFD{ .CLOEXEC = true });
        S.fd = x;
        break :blk x;
    };

    return fd;
}

fn tm2Itimerspec(x: ctime.struct_tm) std.os.linux.itimerspec {
    var mut_x = x;
    const res = ctime.timegm(&mut_x);

    return std.os.linux.itimerspec{
        .it_interval = std.os.linux.timespec{ .tv_sec = 0, .tv_nsec = 0 },
        .it_value = std.os.linux.timespec{ .tv_sec = res, .tv_nsec = 0 },
    };
}

const IoUring = std.os.linux.IoUring;

var uring: IoUring = undefined;

fn processDbusEvents(ring: *IoUring, bus: *sd_bus.sd_bus) !void {
    const fd = blk: {
        const res = sd_bus.sd_bus_get_fd(bus);
        std.debug.assert(0 <= res);
        break :blk res;
    };

    while (true) {
        const res = sd_bus.sd_bus_process(bus, null);
        std.debug.assert(0 <= res);
        if (res == 0) {
            break;
        }
    }

    const sqe = try ring.poll_add(dbus_event_code, fd, std.os.linux.POLL.IN);
    _ = sqe;

    const n = try uring.submit();
    std.debug.assert(n == 1);
}

const dbus_event_code: u64 = 0;
const timer_event_code: u64 = 1;
const child_process_event_code: u64 = 2;

fn armTimer(calt: caltime, dev: *Device) !void {
    try std.posix.timerfd_settime(
        dev.timerfd,
        std.os.linux.TFD.TIMER{ .ABSTIME = true },
        &tm2Itimerspec(calt),
        null,
    );

    const user_data = (dev.pos << 2 | timer_event_code);

    const sqe = try uring.poll_add(user_data, dev.timerfd, std.os.linux.POLL.IN);
    _ = sqe;
    const n = try uring.submit();
    std.debug.assert(n == 1);
}

fn runLoop() !void {
    const bus = get_sd_bus();

    try startWatchingObjects(bus);

    try processDbusEvents(&uring, bus);

    while (true) {
        const cqe = uring.copy_cqe() catch |err| switch (err) {
            // We get the error below when the system wakes up from sleep. We simply
            // restart the wait to handle it.
            error.SignalInterrupt => continue,
            else => |e| return e,
        };

        switch (cqe.user_data & 0b11) {
            dbus_event_code => {
                try processDbusEvents(&uring, bus);
            },
            timer_event_code => {
                try handleTimerExpiration(cqe.user_data);
            },
            child_process_event_code => {
                try handleProcessTermination(cqe.user_data);
            },
            else => unreachable,
        }
    }
}

fn handleTimerExpiration(user_data: u64) !void {
    const idx = user_data >> 2;

    const dev = &GlobalState.devices[idx];

    if (dev.udisks_info) |inf| {
        if (inf.mount_path != null) {
            try initiateBackup(dev);
            return;
        }
    }

    try requestDevice(dev);
}

fn handleProcessTermination(user_data: u64) !void {
    const idx = user_data >> 2;
    const dev = &GlobalState.devices[idx];

    const bp = switch (dev.state) {
        .in_use => |*x| x,
        else => unreachable,
    };

    const status = try bp.proc.wait();

    switch (status) {
        .Exited => |code| std.debug.assert(code == 0),
        else => unreachable,
    }

    closeNotification(bp.notif);

    std.posix.close(bp.pid_fd);
    std.debug.print("Performed a backup\n", .{});

    dev.state = Device.State.up_to_date;

    const calt = try logBackUpCalt(dev.label);

    const next_backup_calt = nextCaltime(calt);

    try armTimer(next_backup_calt, dev);
}

var database: *sqlite.sqlite3 = undefined;

const notifs = @import("notifs.zig");

pub fn main() !void {
    var manager = notifs.Manager.init();

    manager.waitUntilNameOwned();

    uring = try IoUring.init(64, 0);

    database = try setUpDb();

    createTable(database);

    var config_devices = try getConfigDevices();

    defer config_devices.map.deinit();
    defer alloc.free(config_devices.devices);

    GlobalState.devices = config_devices.devices;
    GlobalState.label_index = config_devices.map;

    defer alloc.free(GlobalState.devices);
    defer GlobalState.label_index.deinit();
    defer GlobalState.dbus_path_index.deinit();

    try runLoop();
}

const udisks_bus_name = "org.freedesktop.UDisks2";
const object_manager_path = "/org/freedesktop/UDisks2";
const object_manager_interface = "org.freedesktop.DBus.ObjectManager";
const filesystem_interface = "org.freedesktop.UDisks2.Filesystem";
const properties_interface = "org.freedesktop.DBus.Properties";

fn interfacesAddedCallback(
    m: ?*sd_bus.sd_bus_message,
    d: ?*anyopaque,
    err: [*c]sd_bus.sd_bus_error,
) callconv(.C) c_int {
    _ = d;
    _ = err;

    if (!got_managed_object_list) {
        std.debug.print("Ignoring signal\n", .{});
        return 0;
    }

    decodeInterfacesAddedMessage(m) catch unreachable;

    return 0;
}

fn interfacesRemovedCallback(
    m: ?*sd_bus.sd_bus_message,
    d: ?*anyopaque,
    err: [*c]sd_bus.sd_bus_error,
) callconv(.C) c_int {
    _ = d;
    _ = err;

    const object_path = blk: {
        var object_path: [*:0]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(m, 'o', @ptrCast(&object_path));
        std.debug.assert(0 < res);
        break :blk object_path;
    };

    {
        const res = sd_bus.sd_bus_message_enter_container(m, 'a', "s");
        std.debug.assert(0 < res);
    }

    while (true) {
        const interface_name = blk: {
            var interface_name: [*:0]const u8 = undefined;
            const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&interface_name));
            std.debug.assert(0 <= res);
            if (res == 0) {
                break;
            }
            break :blk interface_name;
        };

        if (std.mem.orderZ(u8, interface_name, interface_of_interest) == std.math.Order.eq) {
            std.debug.print("The following object is going away: {s}\n", .{object_path});

            const path = std.mem.span(object_path);

            std.debug.print("Log1: {s}\n", .{path});

            const kv = GlobalState.dbus_path_index.fetchRemove(path).?;

            alloc.free(kv.key);

            const dev = kv.value;

            const inf = &dev.udisks_info.?;

            {
                const res = sd_bus.sd_bus_slot_unref(inf.slot);
                std.debug.assert(res == null);
            }

            if (inf.mount_path) |x| {
                alloc.free(x);
            }

            dev.udisks_info = null;
        }
    }

    return 0;
}

var got_managed_object_list = false;

fn get_sd_bus() *sd_bus.sd_bus {
    const bus = blk: {
        var bus: ?*sd_bus.sd_bus = undefined;
        const res = sd_bus.sd_bus_default_system(&bus);
        std.debug.assert(0 <= res);
        break :blk bus.?;
    };

    return bus;
}

fn installCallbacks(bus: *sd_bus.sd_bus) void {
    {
        const res = sd_bus.sd_bus_match_signal(
            bus,
            null,
            udisks_bus_name,
            object_manager_path,
            object_manager_interface,
            "InterfacesAdded",
            interfacesAddedCallback,
            null,
        );
        std.debug.assert(0 <= res);
    }
    {
        const res = sd_bus.sd_bus_match_signal(
            bus,
            null,
            udisks_bus_name,
            object_manager_path,
            object_manager_interface,
            "InterfacesRemoved",
            interfacesRemovedCallback,
            null,
        );
        std.debug.assert(0 <= res);
    }
}

fn readInitialList(bus: *sd_bus.sd_bus) !void {
    const reply = callMethod(bus, udisks_bus_name, object_manager_path, object_manager_interface, "GetManagedObjects");

    try decodeManagedObjectReply(reply);

    got_managed_object_list = true;
}

fn startWatchingObjects(bus: *sd_bus.sd_bus) !void {
    installCallbacks(bus);
    try readInitialList(bus);
}

fn decodeInterfacesAddedMessage(m: ?*sd_bus.sd_bus_message) !void {
    const object_path = blk: {
        var object_path: [*c]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(m, 'o', @ptrCast(&object_path));
        std.debug.assert(0 < res);
        break :blk object_path;
    };

    {
        const res = sd_bus.sd_bus_message_enter_container(m, 'a', "{sa{sv}}");
        std.debug.assert(0 < res);
    }

    while (true) {
        {
            const res = sd_bus.sd_bus_message_enter_container(m, 'e', "sa{sv}");
            if (res == 0) {
                break;
            }
            std.debug.assert(0 < res);
        }

        const interface_name = blk: {
            var interface_name: [*:0]const u8 = undefined;
            const res = sd_bus.sd_bus_message_read_basic(m, 's', @ptrCast(&interface_name));
            std.debug.assert(0 < res);
            break :blk interface_name;
        };

        if (std.mem.orderZ(u8, interface_name, interface_of_interest) == std.math.Order.eq) {
            const bus = sd_bus.sd_bus_message_get_bus(m).?;

            const label = getFsLabel(bus, object_path);

            const label_str = std.mem.span(label.string);

            if (GlobalState.label_index.get(label_str)) |p| {
                std.debug.print("We start watching {s}\n", .{label.string});

                try start_watching_device(bus, object_path, p);

                const path = try alloc.dupe(u8, std.mem.span(object_path));

                std.debug.print("Log2: {s}\n", .{path});

                try GlobalState.dbus_path_index.put(path, p);
            }

            const res = sd_bus.sd_bus_message_unref(label.owned_message);
            std.debug.assert(res == null);
        }

        // We don't try to look at the value of the mount point property sent through this
        // message because we're not subscribed to signals yet, so we might miss a change.

        {
            const res = sd_bus.sd_bus_message_skip(m, "a{sv}");
            std.debug.assert(0 <= res);
        }

        {
            const res = sd_bus.sd_bus_message_exit_container(m);
            std.debug.assert(res == 1);
        }
    }

    {
        const res = sd_bus.sd_bus_message_exit_container(m);
        std.debug.assert(res == 1);
    }
}

const interface_of_interest = "org.freedesktop.UDisks2.Filesystem";

fn callMethod(
    bus: ?*sd_bus.sd_bus,
    bus_name: [:0]const u8,
    object_path: [:0]const u8,
    interface_name: [:0]const u8,
    member: [:0]const u8,
) *sd_bus.sd_bus_message {
    var request: ?*sd_bus.sd_bus_message = undefined;

    {
        const res = sd_bus.sd_bus_message_new_method_call(bus, &request, bus_name, object_path, interface_name, member);
        std.debug.assert(0 <= res);
    }

    var err = null_error;
    var reply: ?*sd_bus.sd_bus_message = undefined;
    {
        const res = sd_bus.sd_bus_call(bus, request, 0, &err, &reply);
        std.debug.assert(0 < res);
    }

    {
        const res = sd_bus.sd_bus_message_unref(request);
        std.debug.assert(res == null);
    }

    return reply.?;
}

fn decodeManagedObjectReply(m: ?*sd_bus.sd_bus_message) !void {
    const sig = sd_bus.sd_bus_message_get_signature(m, 1);

    std.debug.assert(std.mem.eql(u8, std.mem.span(sig), "a{oa{sa{sv}}}"));

    {
        const res = sd_bus.sd_bus_message_enter_container(m, 'a', "{oa{sa{sv}}}");
        std.debug.assert(0 < res);
    }

    while (true) {
        {
            const res = sd_bus.sd_bus_message_enter_container(m, 'e', "oa{sa{sv}}");
            if (res == 0) {
                break;
            }
            std.debug.assert(0 < res);
        }

        try decodeInterfacesAddedMessage(m);

        {
            const res = sd_bus.sd_bus_message_exit_container(m);
            std.debug.assert(res == 1);
        }
    }

    const res = sd_bus.sd_bus_message_unref(m);
    std.debug.assert(res == null);
}

const FsLabel = struct {
    string: [*:0]const u8,
    owned_message: *sd_bus.sd_bus_message,
};

const block_interface = "org.freedesktop.UDisks2.Block";

fn getFsLabel(bus: *sd_bus.sd_bus, object_path: [*:0]const u8) FsLabel {
    const request = blk: {
        var request: ?*sd_bus.sd_bus_message = undefined;
        const res = sd_bus.sd_bus_message_new_method_call(
            bus,
            &request,
            udisks_bus_name,
            object_path,
            properties_interface,
            "Get",
        );
        std.debug.assert(0 <= res);
        break :blk request.?;
    };

    {
        const res = sd_bus.sd_bus_message_append_basic(request, 's', @ptrCast(block_interface));
        std.debug.assert(0 <= res);
    }

    {
        const res = sd_bus.sd_bus_message_append_basic(request, 's', @ptrCast("IdLabel"));
        std.debug.assert(0 <= res);
    }

    const reply = blk: {
        var reply: ?*sd_bus.sd_bus_message = undefined;
        var err = null_error;
        const res = sd_bus.sd_bus_call(bus, request, 0, &err, &reply);
        std.debug.assert(0 <= res);
        break :blk reply.?;
    };

    {
        const res = sd_bus.sd_bus_message_unref(request);
        std.debug.assert(res == null);
    }

    const label = blk: {
        {
            const res = sd_bus.sd_bus_message_enter_container(reply, 'v', "s");
            std.debug.assert(0 < res);
        }

        var label: [*:0]const u8 = undefined;
        const res = sd_bus.sd_bus_message_read_basic(reply, 's', @ptrCast(&label));
        std.debug.assert(0 < res);
        break :blk label;
    };

    return FsLabel{ .string = label, .owned_message = reply };
}

const GlobalState = struct {
    var devices: []Device = undefined;
    var label_index: StrIndex = undefined;
    var dbus_path_index = StrIndex.init(alloc);
};

const sqlite = @cImport({
    @cInclude("sqlite3.h");
});

fn setUpDb() !*sqlite.sqlite3 {
    const path = try xdgHelper(xdg_state_dir, "data.db");
    defer alloc.free(path);

    var db: ?*sqlite.sqlite3 = undefined;

    const res = sqlite.sqlite3_open(path, &db);

    std.debug.assert(res == sqlite.SQLITE_OK);

    return db.?;
}

fn createTable(db: *sqlite.sqlite3) void {
    const sql_statement = @embedFile("create-dates-table.sql");

    const res = sqlite.sqlite3_exec(db, sql_statement, null, null, null);

    std.debug.assert(res == sqlite.SQLITE_OK);
}

fn requestDevice(dev: *Device) !void {
    const summary = "Device required";

    const body = blk: {
        const slices = &[_][]const u8{ "Please plug in device <i>", dev.label, "</i>" };

        break :blk try std.mem.joinZ(alloc, "", slices);
    };
    defer alloc.free(body);

    const id = sendNotification(summary, body, null);

    const req = PlugInRequest{ .notif_id = id };

    dev.state = Device.State{ .waited_for = req };
}

fn spawnBackupProcess(dev: *Device) !struct {
    proc: std.process.Child,
    pid_fd: std.posix.fd_t,
} {
    const ChildProcess = std.process.Child;

    const exe_path = try xdgHelper(xdg_config_dir, "perform-backup");
    defer alloc.free(exe_path);

    const mount_path = dev.udisks_info.?.mount_path.?;

    const argv_array = [_][]const u8{ exe_path, mount_path };

    var child = ChildProcess.init(&argv_array, alloc);

    try child.spawn();

    const pid = child.id;

    const u: usize = std.os.linux.pidfd_open(pid, 0);

    const i: isize = @bitCast(u);

    const fd: std.posix.fd_t = @intCast(i);

    const user_data = (dev.pos << 2 | child_process_event_code);

    const sqe = try uring.poll_add(user_data, fd, std.os.linux.POLL.IN);
    _ = sqe;

    const n = try uring.submit();
    std.debug.assert(n == 1);

    return .{ .proc = child, .pid_fd = fd };
}

fn initiateBackup(dev: *Device) !void {
    const summary = "Performing backup";

    const body = try std.mem.joinZ(
        alloc,
        "",
        &[_][]const u8{ "Backup to <i>", dev.label, "</i> in progress" },
    );

    const replaces_id = switch (dev.state) {
        Device.State.waited_for => |req| req.notif_id,
        else => null,
    };

    const notif_id = sendNotification(summary, body, replaces_id);

    const ah = try spawnBackupProcess(dev);

    const bp = BackupProcess{ .proc = ah.proc, .pid_fd = ah.pid_fd, .notif = notif_id };

    dev.state = Device.State{ .in_use = bp };
}
